/**
 * Per-connection bridge event feed over the dsh 0.1.5 Typert Gateway.
 *
 * Three sources replace the old apiproxy `events.mux`:
 *  - `$events`: forwarded Cordis events (waterfalls become the legacy
 *    `question/requested` frames the extension consumes; `emit` frames are
 *    dropped — the extension never consumed the host remote-event shape);
 *  - `session/follow`: per-session durable event streams, relayed as the
 *    legacy `session/event` frames (opening snapshot skipped for parity with
 *    the old mux, which only carried live events);
 *  - `workspace/follow`: cached baseline serving the removed
 *    `workspace.list` unary.
 *
 * @module @yuxianglin/dsh-bridge-browser/src/events
 */

import { randomUUID } from 'node:crypto'
import type { TypertGatewayLike } from './typert-types.ts'
import type { RespondResult } from './protocol.ts'

/** Legacy mux envelope shape carried inside a bridge `event` frame. */
export interface LegacyEventFrame {
  rpcId: string
  method: string
  payload: unknown
}

/** Receives legacy event frames (bound to the active bridge connection). */
export type EventSink = (frame: LegacyEventFrame) => void

/** Stable receipt vocabulary the extension panel already understands. */
export type RespondReceipt = { accepted: true } | { accepted: false; reason: 'not-pending' | 'bad-response' }

/** The gateway's carrier-adjacent `$events/result` dispatcher (unpublished on the typed face). */
interface EventResultDispatcher {
  dispatchRpc(endpoint: string, payload: { args: Record<string, unknown> }, signal?: AbortSignal):
    Promise<{ ok: boolean; value?: unknown; error?: { code: string; message: string; details: unknown } }>
}

/** One waterfall frame item from the `$events` stream. */
interface WaterfallItem {
  type: 'waterfall'
  event: string
  eventId: string
  agentId: string
  request: { questions?: unknown }
}

/** One ready frame item from the `$events` stream. */
interface ReadyItem {
  type: 'ready'
  clientId: string
  host?: unknown
}

/** One cancellation frame item from the `$events` stream. */
interface CancelItem {
  type: 'cancel'
  eventId: string
}

/**
 * Owns one connection generation's stream subscriptions. Created by the
 * server per promoted connection; `close()` tears every stream down.
 */
export class BridgeEventFeed {
  private readonly abort = new AbortController()
  private clientId: string | undefined
  private readonly questionSessions = new Map<string, string>()
  private readonly follows = new Map<string, { abort: AbortController; done: Promise<void> }>()
  private baseline: { items: unknown[]; archivedSessionIds: string[] } = { items: [], archivedSessionIds: [] }

  constructor(
    private readonly gateway: TypertGatewayLike,
    private readonly sink: EventSink,
    private readonly onError?: (code: string, message: string) => void,
  ) {}

  /** Current `workspace/list` baseline (serves the removed unary endpoint). */
  workspaceBaseline(): { items: unknown[]; archivedSessionIds: string[] } {
    return this.baseline
  }

  /** Start every subscription. Returns once the initial session set is requested. */
  async start(): Promise<void> {
    const signal = this.abort.signal
    void this.pumpEvents(signal)
    void this.pumpWorkspace(signal)
    try {
      const result = await this.gateway.invoke({ namespace: 'session', method: 'list', args: { _request: {} }, signal })
      const items = (result as { items?: unknown[] }).items
      if (Array.isArray(items)) {
        for (const item of items) {
          if (isRecord(item) && typeof item.sessionId === 'string') this.noteSession(item.sessionId)
        }
      }
    } catch {
      // A list failure must not break the connection; live events simply
      // start on the next create/prompt observation.
    }
  }

  /** Subscribe durable events for one session (idempotent per feed). */
  noteSession(sessionId: string): void {
    if (sessionId === '' || this.follows.has(sessionId) || this.abort.signal.aborted) return
    const streamAbort = new AbortController()
    const done = this.pumpFollow(sessionId, streamAbort.signal)
    this.follows.set(sessionId, { abort: streamAbort, done })
  }

  /** Answer or cancel one pending waterfall by its legacy rpcId. */
  async answer(rpcId: string, result: RespondResult): Promise<RespondReceipt> {
    const clientId = this.clientId
    if (clientId === undefined || !this.questionSessions.has(rpcId)) return { accepted: false, reason: 'not-pending' }
    let outcome: Record<string, unknown>
    if (result.ok) {
      const value = isRecord(result.value) ? result.value : {}
      const answer = isRecord(value.answer) ? value.answer : {}
      outcome = { kind: 'result', value: answer }
    } else {
      // The old host translated `code === 'cancelled'` into ASK_CANCELLED and
      // refused every other code; keep both behaviors on the new carrier.
      if (result.error.code !== 'cancelled') return { accepted: false, reason: 'bad-response' }
      outcome = {
        kind: 'rejected',
        error: {
          name: 'UserQuestionError',
          message: result.error.message,
          code: 'ASK_CANCELLED',
        },
      }
    }
    const dispatcher = this.gateway as unknown as EventResultDispatcher
    const response = await dispatcher.dispatchRpc('$events/result', { args: { clientId, eventId: rpcId, outcome } })
    if (!response.ok) return { accepted: false, reason: 'not-pending' }
    return { accepted: true }
  }

  /** Tear down every stream; in-flight pumps observe the abort and return. */
  close(): void {
    this.abort.abort()
    for (const follow of this.follows.values()) follow.abort.abort()
  }

  private async pumpEvents(signal: AbortSignal): Promise<void> {
    try {
      const source = await this.gateway.wireStream.open('$events', { args: {} }, signal)
      for await (const item of source) this.handleEventItem(item)
    } catch (error: unknown) {
      if (signal.aborted) return
      this.fail('stream-failed', this.gateway.wireStream.failure(error))
    }
  }

  private handleEventItem(item: unknown): void {
    if (!isRecord(item)) return
    switch (item.type) {
      case 'ready': {
        const ready = item as unknown as ReadyItem
        this.clientId = ready.clientId
        break
      }
      case 'waterfall': {
        const waterfall = item as unknown as WaterfallItem
        if (waterfall.event !== 'user-questions/request') return
        const sessionId = waterfall.agentId
        this.questionSessions.set(waterfall.eventId, sessionId)
        this.sink({
          rpcId: waterfall.eventId,
          method: 'question/requested',
          payload: { sessionId, questions: waterfall.request.questions ?? [] },
        })
        break
      }
      case 'cancel': {
        const cancel = item as unknown as CancelItem
        const sessionId = this.questionSessions.get(cancel.eventId) ?? ''
        this.questionSessions.delete(cancel.eventId)
        this.sink({
          rpcId: cancel.eventId,
          method: 'question/resolved',
          payload: { sessionId, questionRpcId: cancel.eventId },
        })
        break
      }
      default:
        // `emit` frames and unknown additions are ignored: the extension
        // consumes only question and session/event frames.
        break
    }
  }

  private async pumpFollow(sessionId: string, signal: AbortSignal): Promise<void> {
    try {
      const source = await this.gateway.wireStream.open(
        'session/follow',
        { args: { request: { address: { kind: 'session', sessionId } } } },
        signal,
      )
      for await (const item of source) {
        if (!isRecord(item) || item.type !== 'event') continue
        // Parity with the old mux: only live events ride the feed; the
        // opening snapshot is skipped (history is replayed via rpc).
        this.sink({ rpcId: randomUUID(), method: 'session/event', payload: { sessionId, event: item.event } })
      }
    } catch (error: unknown) {
      // Provisional sessions that never materialize fail with
      // session/not-found; close those quietly.
      const wire = this.gateway.wireStream.failure(error)
      if (wire.code !== 'session/not-found' && !signal.aborted) {
        this.fail('stream-failed', wire)
      }
    } finally {
      this.follows.delete(sessionId)
    }
  }

  private async pumpWorkspace(signal: AbortSignal): Promise<void> {
    try {
      const source = await this.gateway.wireStream.open('workspace/follow', { args: {} }, signal)
      for await (const item of source) {
        if (!isRecord(item) || item.type !== 'baseline' || !isRecord(item.value)) continue
        const value = item.value as { items?: unknown; archivedSessionIds?: unknown }
        this.baseline = {
          items: Array.isArray(value.items) ? value.items : [],
          archivedSessionIds: Array.isArray(value.archivedSessionIds)
            ? value.archivedSessionIds.map(String)
            : [],
        }
      }
    } catch {
      // workspace.list then serves the empty baseline; grouping stays off.
    }
  }

  private fail(code: string, wire: { code: string; message: string }): void {
    // Fatal pump failures keep the old top-level error-frame semantics (the
    // extension re-authenticates on `{ t: 'error' }`); without a handler the
    // failure still lands as a legacy stream/error event frame.
    if (this.onError !== undefined) {
      this.onError(code, `${wire.code}: ${wire.message}`)
      return
    }
    this.sink({
      rpcId: randomUUID(),
      method: 'stream/error',
      payload: { error: { code, message: `${wire.code}: ${wire.message}` } },
    })
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
