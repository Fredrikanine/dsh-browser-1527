/**
 * Legacy apiproxy method vocabulary → dsh 0.1.5 Typert Gateway adapter.
 *
 * The bridge wire protocol (protocol.ts) stays frozen for the extension, so
 * this module owns the translation layer only: old dot-notation method names
 * and request/response shapes are mapped onto Typert Remote endpoints and
 * back onto the legacy ServerResponse envelope the panel expects.
 *
 * Unary calls ride `ctx.typertGateway.invoke` (the same dispatch the /api
 * carrier uses, without an HTTP hop).
 *
 * @module @yuxianglin/dsh-bridge-browser/src/gateway
 */

import { randomUUID } from 'node:crypto'
import { mkdir } from 'node:fs/promises'
import type { Context } from '@deepseek-ai/cordis'
import type { ImageAttachmentLimits } from '@deepseek-ai/dsh-attachment'
import type { InvokeRemoteRequestLike, TypertGatewayLike } from './typert-types.ts'

/** Legacy result envelope (mirrors the old apiproxy ServerResponse.result). */
export type LegacyResult =
  | { ok: true; value: unknown }
  | { ok: false; error: { code: string; message: string; details: Record<string, unknown> } }

/** Method-name → legacy-request dispatcher, consumed by the bridge server. */
export type LegacyInvoker = (method: string, payload: unknown, signal?: AbortSignal) => Promise<LegacyResult>

/** Called with a durable session id the bridge should subscribe live events for. */
export type SessionObserver = (sessionId: string) => void

/** Old session.create payload shape (all fields optional). */
interface CreatePayload {
  sessionId?: string
  cwd?: string
  workspaceId?: string
  agentPreset?: string
}

/**
 * Project a gateway failure into the legacy error vocabulary. Business
 * failures already carry stable codes; everything else folds to internal.
 */
function legacyFailure(error: unknown, gateway: TypertGatewayLike): LegacyResult {
  const wire = gateway.wireStream.failure(error)
  return {
    ok: false,
    error: { code: wire.code, message: wire.message, details: { ...wire.details } },
  }
}

/**
 * Session history in the old vocabulary: the panel reads
 * `history.events.map((entry) => entry.event)` plus `projections.imageLimits`.
 */
interface HistoryPage {
  events: Array<{ type: 'event'; event: unknown }>
  hasMore: boolean
  projections: { asOfSeq: number; values: Record<string, unknown> }
}

/**
 * Build the legacy invoker. `deferSessionCreate`/`sessionWorkspacePath` are
 * the two old ApiProxy wrappers re-expressed at the frame level; `imageLimits`
 * seeds the synthetic empty history for provisional sessions.
 */
export function createLegacyInvoker(
  ctx: Context,
  options: {
    deferSessionCreate: boolean
    sessionWorkspacePath: string
    imageLimits?: ImageAttachmentLimits
    warn: (message: string) => void
    observeSession: SessionObserver
    workspaceBaseline: () => { items: unknown[]; archivedSessionIds: string[] }
  },
): LegacyInvoker {
  const gateway = ctx.typertGateway
  const deferral = createDeferralLayer(options.deferSessionCreate, options.imageLimits)
  const grouping = createWorkspaceGrouping(gateway, options.sessionWorkspacePath, options.warn, options.workspaceBaseline)

  const invoke = async (namespace: string, method: string, args: Record<string, unknown>, signal?: AbortSignal): Promise<LegacyResult> => {
    const request: InvokeRemoteRequestLike = { namespace, method, args, ...(signal === undefined ? {} : { signal }) }
    try {
      return { ok: true, value: await gateway.invoke(request) }
    } catch (error: unknown) {
      return legacyFailure(error, gateway)
    }
  }

  const translateHistory = async (sessionId: string, signal?: AbortSignal): Promise<LegacyResult> => {
    const result = await invoke(
      'session',
      'page',
      { request: { address: { kind: 'session', sessionId }, throughSeq: -1 } },
      signal,
    )
    if (!result.ok) return result
    const page = result.value as { records?: unknown[]; hasMore?: boolean }
    const history: HistoryPage = {
      events: Array.isArray(page.records) ? (page.records as HistoryPage['events']) : [],
      hasMore: page.hasMore ?? false,
      projections: {
        asOfSeq: -1,
        values: options.imageLimits === undefined ? {} : { imageLimits: options.imageLimits },
      },
    }
    return { ok: true, value: history }
  }

  const createSession = async (payload: CreatePayload, signal?: AbortSignal): Promise<LegacyResult> => {
    const request = await grouping.create(payload)
    const result = await deferral.create(request)
    if (result !== null) {
      options.observeSession(result.sessionId)
      return { ok: true, value: { sessionId: result.sessionId } }
    }
    const created = await invoke('session', 'create', { request }, signal)
    if (created.ok) {
      const sessionId = (created.value as { sessionId?: unknown }).sessionId
      if (typeof sessionId === 'string') options.observeSession(sessionId)
    }
    return created
  }

  const promptSession = async (payload: Record<string, unknown>, signal?: AbortSignal): Promise<LegacyResult> => {
    const sessionId = typeof payload.sessionId === 'string' ? payload.sessionId : ''
    const materialized = await deferral.prompt(sessionId, (request) =>
      invoke('session', 'create', { request }, signal))
    if (materialized !== null) return materialized
    const result = await invoke('session', 'prompt', { request: payload }, signal)
    if (result.ok && sessionId !== '') options.observeSession(sessionId)
    return result
  }

  const translate = (method: string, payload: unknown, signal?: AbortSignal): Promise<LegacyResult> => {
    switch (method) {
      case 'session.list':
        return invoke('session', 'list', { _request: isRecord(payload) ? payload : {} }, signal)
      case 'session.create':
        return createSession(isRecord(payload) ? (payload as CreatePayload) : {}, signal)
      case 'session.history': {
        const sessionId = isRecord(payload) ? (payload as { sessionId?: unknown }).sessionId : ''
        if (typeof sessionId !== 'string' || sessionId === '') {
          return Promise.resolve({ ok: false, error: { code: 'bad-request', message: 'sessionId must be a non-empty string', details: {} } })
        }
        return deferral.history(sessionId, (id) => translateHistory(id, signal))
      }
      case 'session.prompt':
        return promptSession(isRecord(payload) ? payload : {}, signal)
      case 'session.cancel': {
        const sessionId = isRecord(payload) ? (payload as { sessionId?: unknown }).sessionId : undefined
        if (typeof sessionId !== 'string') {
          return Promise.resolve({ ok: false, error: { code: 'bad-request', message: 'sessionId must be a string', details: {} } })
        }
        return invoke('session', 'cancel', { request: { sessionId } }, signal)
      }
      case 'workspace.list':
        return Promise.resolve({ ok: true, value: grouping.list() })
      case 'workspace.archiveSession': {
        const sessionId = isRecord(payload) ? (payload as { sessionId?: unknown }).sessionId : undefined
        if (typeof sessionId !== 'string') {
          return Promise.resolve({ ok: false, error: { code: 'bad-request', message: 'sessionId must be a string', details: {} } })
        }
        return invoke('workspace', 'archiveSession', { request: { sessionId } }, signal)
      }
      case 'llm.discoverModels': {
        if (!isRecord(payload)) {
          return Promise.resolve({ ok: false, error: { code: 'bad-request', message: 'discoverModels payload must be an object', details: {} } })
        }
        const { settingsNs, provider, api, baseURL, apiKey } = payload as Record<string, unknown>
        if (typeof settingsNs !== 'string') {
          return Promise.resolve({ ok: false, error: { code: 'bad-request', message: 'settingsNs must be a string', details: {} } })
        }
        const request: Record<string, unknown> = {}
        if (typeof provider === 'string') request.provider = provider
        if (typeof api === 'string') request.api = api
        if (typeof baseURL === 'string') request.baseURL = baseURL
        if (typeof apiKey === 'string') request.apiKey = apiKey
        return invoke('llm', 'discoverModels', { settingsNs, request }, signal)
          .then((value) => (value.ok ? { ok: true as const, value: { models: value.value } } : value))
      }
      case 'settings.describe':
        return invoke('settings', 'describe', {}, signal)
      default:
        return Promise.resolve({
          ok: false,
          error: { code: 'method-unavailable', message: `bridge does not relay gateway method "${method}"`, details: {} },
        })
    }
  }

  return (method, payload, signal) => {
    try {
      return translate(method, payload, signal)
    } catch (error: unknown) {
      return Promise.resolve(legacyFailure(error, gateway))
    }
  }
}

/**
 * Frame-level re-expression of the old withSessionDeferral wrapper:
 * `session.create` answers with a provisional id (nothing persisted),
 * `session.history` serves provisional ids as empty, and the real session
 * materializes — same id, original create payload — on the first prompt.
 */
function createDeferralLayer(
  enabled: boolean,
  imageLimits?: ImageAttachmentLimits,
): {
  create: (payload: CreatePayload) => { sessionId: string } | null
  history: (sessionId: string, translateHistory: (sessionId: string) => Promise<LegacyResult>) => Promise<LegacyResult>
  prompt: (sessionId: string, invokeCreate: (request: CreatePayload) => Promise<LegacyResult>) => Promise<LegacyResult | null>
} {
  const PROVISIONAL_TTL_MS = 30 * 60_000
  const provisional = new Map<string, { payload: CreatePayload; createdAt: number }>()
  const materializing = new Map<string, Promise<LegacyResult>>()

  const prune = (): void => {
    const cutoff = Date.now() - PROVISIONAL_TTL_MS
    for (const [id, entry] of provisional) {
      if (entry.createdAt < cutoff) provisional.delete(id)
    }
  }

  return {
    create(payload) {
      prune()
      if (!enabled) return null
      const sessionId = payload.sessionId ?? `session-${randomUUID()}`
      provisional.set(sessionId, { payload: { ...payload }, createdAt: Date.now() })
      return { sessionId }
    },
    async history(sessionId, translateHistory) {
      if (!provisional.has(sessionId)) return translateHistory(sessionId)
      return {
        ok: true,
        value: {
          events: [],
          hasMore: false,
          projections: {
            asOfSeq: -1,
            values: imageLimits === undefined ? {} : { imageLimits },
          },
        },
      }
    },
    async prompt(sessionId, invokeCreate) {
      const entry = provisional.get(sessionId)
      if (entry === undefined) return null
      const existing = materializing.get(sessionId)
      const pending = existing ?? invokeCreate({ ...entry.payload, sessionId })
      if (existing === undefined) {
        materializing.set(sessionId, pending)
        void pending.then(
          () => { materializing.delete(sessionId) },
          () => { materializing.delete(sessionId) },
        )
      }
      const created = await pending
      provisional.delete(sessionId)
      return created.ok ? null : created
    },
  }
}

/**
 * Frame-level re-expression of the old withSessionWorkspace wrapper:
 * implicit `session.create` requests get the dedicated workspace attached.
 * `workspace.list` (removed in 0.1.5) is served from the workspace/follow
 * baseline cached by the events hub.
 */
function createWorkspaceGrouping(
  gateway: TypertGatewayLike,
  workspacePath: string,
  warn: (message: string) => void,
  workspaceBaseline: () => { items: unknown[]; archivedSessionIds: string[] },
): {
  create: (request: CreatePayload) => Promise<CreatePayload>
  list: () => { workspaces: unknown[]; archivedSessionIds: string[] }
} {
  let workspacePromise: Promise<string | undefined> | undefined

  const ensureWorkspace = (): Promise<string | undefined> => {
    if (workspacePromise !== undefined) return workspacePromise
    workspacePromise = (async () => {
      try {
        await mkdir(workspacePath, { recursive: true })
        const value = await gateway.invoke({ namespace: 'workspace', method: 'create', args: { request: { path: workspacePath } } })
        const workspace = (value as { workspace?: { workspaceId?: unknown } }).workspace
        if (workspace === undefined || typeof workspace.workspaceId !== 'string') {
          warn(`browser bridge: workspace.create returned no workspaceId for "${workspacePath}"; sessions will remain ungrouped`)
          return undefined
        }
        return workspace.workspaceId
      } catch (error: unknown) {
        warn(`browser bridge: could not prepare session workspace "${workspacePath}": ${String(error)}; sessions will remain ungrouped`)
        return undefined
      }
    })()
    return workspacePromise
  }

  return {
    async create(request) {
      if (workspacePath === '' || request.workspaceId !== undefined) return request
      const workspaceId = await ensureWorkspace()
      if (workspaceId === undefined) return request
      const payload = { ...request, workspaceId }
      delete payload.cwd
      return payload
    },
    list() {
      const baseline = workspaceBaseline()
      return { workspaces: baseline.items, archivedSessionIds: baseline.archivedSessionIds }
    },
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
