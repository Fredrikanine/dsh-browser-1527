/**
 * Gateway adapter unit coverage: legacy method vocabulary → Typert endpoints,
 * legacy envelope projection, deferral materialization, and workspace
 * grouping, all against a recording fake gateway.
 */

import { describe, expect, it, vi } from 'vitest'
import { createLegacyInvoker, type LegacyResult } from '../src/gateway.ts'
import type { TypertGatewayLike } from '../src/typert-types.ts'

interface RecordingGateway extends TypertGatewayLike {
  invoke: ReturnType<typeof vi.fn>
  calls: Array<{ namespace: string; method: string; args: Record<string, unknown> }>
}

/** Fake gateway recording every invoke and answering from a queue. */
function recordingGateway(responses: Array<unknown | Error> = []): RecordingGateway {
  const invoke = vi.fn(async (request: { namespace: string; method: string; args: Record<string, unknown> }) => {
    const call = { namespace: request.namespace, method: request.method, args: request.args }
    calls.push(call)
    const next = responses.shift()
    if (next instanceof Error) throw next
    return next ?? { accepted: true }
  })
  const calls: Array<{ namespace: string; method: string; args: Record<string, unknown> }> = []
  return {
    invoke,
    calls,
    stream: vi.fn(async () => ({ async *[Symbol.asyncIterator]() {} })),
    wireStream: {
      open: vi.fn(async () => ({ async *[Symbol.asyncIterator]() {} })),
      failure: (error: unknown) => {
        const candidate = error as { isDSHRemoteError?: boolean; code?: string; message?: string; details?: unknown }
        if (candidate !== null && typeof candidate === 'object' && candidate.isDSHRemoteError === true && typeof candidate.code === 'string') {
          return { code: candidate.code, message: candidate.message ?? '', details: (candidate.details ?? {}) as object }
        }
        return {
          code: 'gateway/internal',
          message: error instanceof Error ? error.message : String(error),
          details: {},
        }
      },
    },
  }
}

function invoker(gateway: TypertGatewayLike, overrides: Partial<Parameters<typeof createLegacyInvoker>[1]> = {}) {
  return createLegacyInvoker(
    { typertGateway: gateway } as never,
    {
      deferSessionCreate: false,
      sessionWorkspacePath: '',
      warn: () => {},
      observeSession: () => {},
      workspaceBaseline: () => ({ items: [], archivedSessionIds: [] }),
      ...overrides,
    },
  )
}

function okValue(result: LegacyResult): unknown {
  if (!result.ok) throw new Error(`expected ok result, got ${JSON.stringify(result)}`)
  return result.value
}

describe('legacy → Typert translation', () => {
  it('maps session.list to session/list with the reserved _request field', async () => {
    const gateway = recordingGateway([{ items: [{ sessionId: 's1', running: true }] }])
    const call = invoker(gateway)
    const result = await call('session.list', {})
    expect(result).toEqual({ ok: true, value: { items: [{ sessionId: 's1', running: true }] } })
    expect(gateway.calls).toEqual([{ namespace: 'session', method: 'list', args: { _request: {} } }])
  })

  it('maps session.create through the request field and observes the returned id', async () => {
    const observeSession = vi.fn()
    const gateway = recordingGateway([{ sessionId: 'session-x' }])
    const call = invoker(gateway, { observeSession })
    const result = await call('session.create', { cwd: 'C:/work' })
    expect(okValue(result)).toEqual({ sessionId: 'session-x' })
    expect(gateway.calls).toEqual([{ namespace: 'session', method: 'create', args: { request: { cwd: 'C:/work' } } }])
    expect(observeSession).toHaveBeenCalledWith('session-x')
  })

  it('maps session.history to session/page and projects the legacy page shape', async () => {
    const event = { type: 'user/message', seq: 0, time: 1, data: { content: [] } }
    const gateway = recordingGateway([{ records: [{ type: 'event', event }], hasMore: false }])
    const call = invoker(gateway)
    const result = await call('session.history', { sessionId: 'session-x' })
    expect(result).toEqual({
      ok: true,
      value: {
        events: [{ type: 'event', event }],
        hasMore: false,
        projections: { asOfSeq: -1, values: {} },
      },
    })
    expect(gateway.calls).toEqual([{
      namespace: 'session',
      method: 'page',
      args: { request: { address: { kind: 'session', sessionId: 'session-x' }, throughSeq: -1 } },
    }])
  })

  it('maps session.cancel to the request-wrapped endpoint', async () => {
    const gateway = recordingGateway([{ accepted: true }])
    const call = invoker(gateway)
    const result = await call('session.cancel', { sessionId: 'session-x' })
    expect(result.ok).toBe(true)
    expect(gateway.calls).toEqual([{ namespace: 'session', method: 'cancel', args: { request: { sessionId: 'session-x' } } }])
  })

  it('maps workspace.archiveSession and llm.discoverModels with the new shapes', async () => {
    const gateway = recordingGateway([{ archivedSessionIds: [] }, [{ id: 'm1', name: 'Model 1' }]])
    const call = invoker(gateway)
    expect(await call('workspace.archiveSession', { sessionId: 'session-x' })).toEqual({
      ok: true,
      value: { archivedSessionIds: [] },
    })
    expect(await call('llm.discoverModels', { settingsNs: 'llm-pi-ai', provider: 'p', api: 'openai', baseURL: 'https://x', apiKey: 'k' })).toEqual({
      ok: true,
      value: { models: [{ id: 'm1', name: 'Model 1' }] },
    })
    expect(gateway.calls[0]).toEqual({ namespace: 'workspace', method: 'archiveSession', args: { request: { sessionId: 'session-x' } } })
    expect(gateway.calls[1]).toEqual({
      namespace: 'llm',
      method: 'discoverModels',
      args: { settingsNs: 'llm-pi-ai', request: { provider: 'p', api: 'openai', baseURL: 'https://x', apiKey: 'k' } },
    })
  })

  it('serves workspace.list from the cached workspace/follow baseline', async () => {
    const gateway = recordingGateway()
    const call = invoker(gateway, {
      workspaceBaseline: () => ({ items: [{ workspaceId: 'w1' }], archivedSessionIds: ['session-a'] }),
    })
    const result = await call('workspace.list', {})
    expect(result).toEqual({
      ok: true,
      value: { workspaces: [{ workspaceId: 'w1' }], archivedSessionIds: ['session-a'] },
    })
    expect(gateway.calls).toEqual([])
  })

  it('maps settings.describe to the parameterless endpoint', async () => {
    const gateway = recordingGateway([{ writable: true, hasDocument: false, namespaces: [] }])
    const call = invoker(gateway)
    expect(await call('settings.describe', {})).toEqual({ ok: true, value: { writable: true, hasDocument: false, namespaces: [] } })
    expect(gateway.calls).toEqual([{ namespace: 'settings', method: 'describe', args: {} }])
  })

  it('projects gateway failures into the legacy error envelope', async () => {
    const gateway = recordingGateway()
    gateway.invoke.mockRejectedValueOnce(Object.assign(new Error('session is gone'), {
      isDSHRemoteError: true,
      code: 'session/not-found',
      details: { sessionId: 's' },
    }))
    const call = invoker(gateway)
    const result = await call('session.list', {})
    expect(result).toEqual({
      ok: false,
      error: { code: 'session/not-found', message: 'session is gone', details: { sessionId: 's' } },
    })
  })

  it('rejects unknown methods with method-unavailable', async () => {
    const call = invoker(recordingGateway())
    expect(await call('nope.never', {})).toMatchObject({
      ok: false,
      error: { code: 'method-unavailable' },
    })
  })
})

describe('session deferral', () => {
  it('answers session.create with a provisional id and materializes on the first prompt', async () => {
    const gateway = recordingGateway([{ sessionId: 'session-real' }, { accepted: true }])
    const call = invoker(gateway, { deferSessionCreate: true })

    const created = await call('session.create', { cwd: 'C:/work' })
    expect(created.ok).toBe(true)
    const provisional = (created as { ok: true; value: { sessionId: string } }).value.sessionId
    expect(provisional).toMatch(/^session-[0-9a-f-]{36}$/)
    expect(gateway.calls).toEqual([])

    const history = await call('session.history', { sessionId: provisional })
    expect(history).toEqual({
      ok: true,
      value: { events: [], hasMore: false, projections: { asOfSeq: -1, values: {} } },
    })
    expect(gateway.calls).toEqual([])

    const prompted = await call('session.prompt', { sessionId: provisional, mode: 'queue', content: [{ type: 'text', text: 'hi' }] })
    expect(prompted).toEqual({ ok: true, value: { accepted: true } })
    expect(gateway.calls).toEqual([
      { namespace: 'session', method: 'create', args: { request: { cwd: 'C:/work', sessionId: provisional } } },
      { namespace: 'session', method: 'prompt', args: { request: { sessionId: provisional, mode: 'queue', content: [{ type: 'text', text: 'hi' }] } } },
    ])

    // After materialization the history passthrough reaches the gateway.
    const after = await call('session.history', { sessionId: provisional })
    expect(after.ok).toBe(true)
    expect(gateway.calls.length).toBe(3)
  })

  it('surfaces a materialization failure as the prompt failure', async () => {
    const gateway = recordingGateway()
    gateway.invoke.mockRejectedValueOnce(Object.assign(new Error('cwd conflict'), {
      isDSHRemoteError: true,
      code: 'session/conflict',
      details: { sessionId: 's' },
    }))
    const call = invoker(gateway, { deferSessionCreate: true })
    const created = await call('session.create', {})
    const id = (created as { ok: true; value: { sessionId: string } }).value.sessionId
    const prompted = await call('session.prompt', { sessionId: id, mode: 'queue', content: [] })
    expect(prompted).toMatchObject({ ok: false, error: { code: 'session/conflict' } })
  })

  it('passes through untouched when deferral is disabled', async () => {
    const gateway = recordingGateway([{ sessionId: 'session-direct' }])
    const call = invoker(gateway, { deferSessionCreate: false })
    const result = await call('session.create', {})
    expect(okValue(result)).toEqual({ sessionId: 'session-direct' })
    expect(gateway.calls.length).toBe(1)
  })
})

describe('workspace grouping', () => {
  it('attaches the dedicated workspace to implicit creates', async () => {
    const gateway = recordingGateway([
      { workspace: { workspaceId: 'ws-1' } }, // workspace.create
      { sessionId: 'session-grouped' },      // session.create
    ])
    const call = invoker(gateway, { sessionWorkspacePath: 'C:/browser-sessions' })
    const result = await call('session.create', { cwd: 'C:/work' })
    expect(okValue(result)).toEqual({ sessionId: 'session-grouped' })
    expect(gateway.calls).toEqual([
      { namespace: 'workspace', method: 'create', args: { request: { path: 'C:/browser-sessions' } } },
      { namespace: 'session', method: 'create', args: { request: { workspaceId: 'ws-1' } } },
    ])
  })

  it('caches the workspace id and reuses it for later creates', async () => {
    const gateway = recordingGateway([
      { workspace: { workspaceId: 'ws-1' } },
      { sessionId: 's1' },
      { sessionId: 's2' },
    ])
    const call = invoker(gateway, { sessionWorkspacePath: 'C:/browser-sessions' })
    await call('session.create', {})
    await call('session.create', {})
    expect(gateway.calls).toEqual([
      { namespace: 'workspace', method: 'create', args: { request: { path: 'C:/browser-sessions' } } },
      { namespace: 'session', method: 'create', args: { request: { workspaceId: 'ws-1' } } },
      { namespace: 'session', method: 'create', args: { request: { workspaceId: 'ws-1' } } },
    ])
  })

  it('leaves explicit workspace choices untouched', async () => {
    const gateway = recordingGateway([{ sessionId: 's1' }])
    const call = invoker(gateway, { sessionWorkspacePath: 'C:/browser-sessions' })
    await call('session.create', { workspaceId: 'ws-explicit' })
    expect(gateway.calls).toEqual([
      { namespace: 'session', method: 'create', args: { request: { workspaceId: 'ws-explicit' } } },
    ])
  })
})
