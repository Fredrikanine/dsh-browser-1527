/**
 * BridgeEventFeed unit coverage: `$events` waterfall/cancel mapping, per-
 * session follow relaying, workspace baseline caching, and answer routing.
 */

import { describe, expect, it, vi } from 'vitest'
import { BridgeEventFeed, type LegacyEventFrame } from '../src/events.ts'
import type { TypertGatewayLike } from '../src/typert-types.ts'

type StreamFactory = (signal: AbortSignal) => AsyncIterable<unknown>

interface FakeGateway extends TypertGatewayLike {
  dispatchRpc: ReturnType<typeof vi.fn>
  invoke: ReturnType<typeof vi.fn>
}

function fakeGateway(streams: Record<string, StreamFactory>, invokeResult: unknown = { items: [] }): FakeGateway {
  const dispatchRpc = vi.fn(async () => ({ ok: true }))
  const invoke = vi.fn(async () => invokeResult)
  return {
    invoke,
    stream: vi.fn(async () => ({ async *[Symbol.asyncIterator]() {} })),
    dispatchRpc,
    wireStream: {
      open: vi.fn(async (endpoint: string, _payload: unknown, signal: AbortSignal) => {
        const factory = streams[endpoint]
        if (factory === undefined) throw new Error(`unexpected stream endpoint ${endpoint}`)
        return factory(signal)
      }),
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

function frames(feed: BridgeEventFeed): { sink: ReturnType<typeof vi.fn>; frames: LegacyEventFrame[] } {
  const frames: LegacyEventFrame[] = []
  const sink = vi.fn((frame: LegacyEventFrame) => { frames.push(frame) })
  return { sink, frames }
}

async function settle(ms = 20): Promise<void> {
  await new Promise((resolve) => { setTimeout(resolve, ms) })
}

describe('BridgeEventFeed', () => {
  it('maps $events ready/waterfall/cancel items to legacy question frames', async () => {
    const gateway = fakeGateway({
      '$events': async function* () {
        yield { type: 'ready', clientId: 'client-1', host: { home: 'C:/home' } }
        yield {
          type: 'waterfall',
          event: 'user-questions/request',
          eventId: 'e1',
          agentId: 'session-1',
          request: { questions: [{ id: 'q', question: 'Pick one' }] },
        }
        yield { type: 'emit', event: 'settings/document-updated', args: [{ ns: 'x' }] }
        yield { type: 'cancel', eventId: 'e1' }
      },
    })
    const { sink } = frames({} as never)
    const feed = new BridgeEventFeed(gateway, sink)
    await feed.start()
    await settle()

    expect(sink.mock.calls.map((call) => (call[0] as LegacyEventFrame).method)).toEqual([
      'question/requested',
      'question/resolved',
    ])
    expect(sink.mock.calls[0]![0]).toMatchObject({
      rpcId: 'e1',
      method: 'question/requested',
      payload: { sessionId: 'session-1', questions: [{ id: 'q', question: 'Pick one' }] },
    })
    expect(sink.mock.calls[1]![0]).toMatchObject({
      rpcId: 'e1',
      method: 'question/resolved',
      payload: { sessionId: 'session-1', questionRpcId: 'e1' },
    })
    feed.close()
  })

  it('answers pending waterfalls through $events/result with the unwrapped batch', async () => {
    const gateway = fakeGateway({
      '$events': async function* () {
        yield { type: 'ready', clientId: 'client-2' }
        yield {
          type: 'waterfall',
          event: 'user-questions/request',
          eventId: 'e2',
          agentId: 'session-2',
          request: { questions: [{ id: 'q', question: 'Pick one' }] },
        }
      },
    })
    const { sink } = frames({} as never)
    const feed = new BridgeEventFeed(gateway, sink)
    await feed.start()
    await settle()

    const receipt = await feed.answer('e2', {
      ok: true,
      value: { sessionId: 'session-2', answer: { answers: [{ id: 'q', selected: ['A'] }] } },
    })
    expect(receipt).toEqual({ accepted: true })
    expect(gateway.dispatchRpc).toHaveBeenCalledWith('$events/result', {
      args: {
        clientId: 'client-2',
        eventId: 'e2',
        outcome: { kind: 'result', value: { answers: [{ id: 'q', selected: ['A'] }] } },
      },
    })
    feed.close()
  })

  it('relays session/follow live events as legacy session/event frames and skips the snapshot', async () => {
    const gateway = fakeGateway({
      'session/follow': async function* () {
        yield { type: 'snapshot', header: {}, cursor: 3, records: [], hasMore: false, projections: { asOfSeq: 3, values: {} } }
        yield { type: 'event', event: { type: 'user/message', seq: 4, time: 1, data: { content: [] } } }
        yield { type: 'assistant-stream', frame: {}, ordinal: 1 }
      },
    })
    const { sink } = frames({} as never)
    const feed = new BridgeEventFeed(gateway, sink)
    feed.noteSession('session-3')
    await settle()

    expect(sink.mock.calls.map((call) => (call[0] as LegacyEventFrame).method)).toEqual(['session/event'])
    expect(sink.mock.calls[0]![0]).toMatchObject({
      method: 'session/event',
      payload: { sessionId: 'session-3', event: { type: 'user/message', seq: 4 } },
    })
    feed.close()
  })

  it('opens follow streams for every session returned by the startup list', async () => {
    const gateway = fakeGateway({
      'session/follow': async function* () {
        yield { type: 'snapshot', header: {}, cursor: -1, records: [], hasMore: false, projections: { asOfSeq: -1, values: {} } }
      },
    }, { items: [{ sessionId: 's1' }, { sessionId: 's2' }] })
    const { sink } = frames({} as never)
    const feed = new BridgeEventFeed(gateway, sink)
    await feed.start()
    await settle()
    expect(gateway.invoke).toHaveBeenCalledWith({ namespace: 'session', method: 'list', args: { _request: {} }, signal: expect.any(AbortSignal) })
    expect(gateway.wireStream.open).toHaveBeenCalledTimes(4) // $events + workspace/follow + 2 session follows
    feed.close()
  })

  it('ignores session/not-found follow failures quietly', async () => {
    const gateway = fakeGateway({
      'session/follow': async function* () {
        const error = Object.assign(new Error('provisional session does not exist'), {
          isDSHRemoteError: true,
          code: 'session/not-found',
          details: { sessionId: 'ghost' },
        })
        throw error
      },
    })
    const { sink } = frames({} as never)
    const feed = new BridgeEventFeed(gateway, sink)
    feed.noteSession('ghost')
    await settle()
    expect(sink).not.toHaveBeenCalled()
    feed.close()
  })

  it('caches the workspace/follow baseline for workspace.list', async () => {
    const gateway = fakeGateway({
      'workspace/follow': async function* () {
        yield {
          type: 'baseline',
          value: { items: [{ workspaceId: 'w1' }], archivedSessionIds: ['session-a'] },
        }
      },
    })
    const { sink } = frames({} as never)
    const feed = new BridgeEventFeed(gateway, sink)
    await feed.start()
    await settle()
    expect(feed.workspaceBaseline()).toEqual({
      items: [{ workspaceId: 'w1' }],
      archivedSessionIds: ['session-a'],
    })
    feed.close()
  })

  it('stays silent when the $events stream throws after close', async () => {
    const gateway = fakeGateway({
      '$events': async function* () {
        yield { type: 'ready', clientId: 'c' }
        await new Promise((resolve) => { setTimeout(resolve, 30) })
        throw new Error('late failure')
      },
    })
    const { sink } = frames({} as never)
    const feed = new BridgeEventFeed(gateway, sink)
    await feed.start()
    feed.close()
    await settle(60)
    // The pump failed after close: no error frame escapes.
    expect(sink).not.toHaveBeenCalled()
  })
})
