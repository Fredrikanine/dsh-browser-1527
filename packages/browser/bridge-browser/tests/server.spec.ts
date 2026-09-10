import { createServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { afterEach, describe, expect, it, vi } from 'vitest'
import WebSocket from 'ws'
import { BridgeServer, BridgeToolError, isLoopbackAddress, messageToText, payloadCode, payloadMessage } from '../src/server.ts'
import { BridgeEventFeed } from '../src/events.ts'
import type { LegacyResult } from '../src/gateway.ts'
import type { TypertGatewayLike } from '../src/typert-types.ts'
import { BRIDGE_INJECT_BROWSER_SNAPSHOT_METHOD, BRIDGE_SESSION_PURGE_METHOD, type BridgeFrame } from '../src/protocol.ts'
import { SessionPurgeError } from '../src/session-purge.ts'

const TOKEN = 'deadbeefdeadbeefdeadbeefdeadbeef'

/** 扩展上下文的 Origin（回环免 token 的必要条件）。 */
const EXT_ORIGIN = 'chrome-extension://test-extension-id'
const FIREFOX_EXT_ORIGIN = 'moz-extension://per-install-uuid'

/** Extension caps used by every hello in this suite. */
const CAPS = { textOnly: true as const, snapshotMaxChars: 12_000, maxInteractiveItems: 60 }

/** One controllable stream keyed by endpoint in the fake gateway. */
type StreamFactory = (signal: AbortSignal) => AsyncIterable<unknown>

function emptyStream(): AsyncIterable<unknown> {
  return { async *[Symbol.asyncIterator]() {} }
}

interface FakeGateway extends TypertGatewayLike {
  dispatchRpc: ReturnType<typeof vi.fn>
  streams: Map<string, StreamFactory>
}

/** Test gateway over the structural Typert seam. */
function fakeGateway(streams: Record<string, StreamFactory> = {}): FakeGateway {
  const dispatchRpc = vi.fn(async () => ({ ok: true }))
  return {
    invoke: vi.fn(async ({ namespace, method }: { namespace: string; method: string }) => {
      if (namespace === 'session' && method === 'list') return { items: [] }
      return { accepted: true }
    }),
    stream: vi.fn(async () => emptyStream()),
    dispatchRpc,
    streams: new Map(Object.entries(streams)),
    wireStream: {
      open: vi.fn(async (endpoint: string, _payload: unknown, signal: AbortSignal) => {
        const factory = streams[endpoint]
        return factory === undefined ? emptyStream() : factory(signal)
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

interface Harness {
  bridge: BridgeServer
  server: Server
  url: string
  invokeRpc: ReturnType<typeof vi.fn>
  gateway: FakeGateway
}

async function startBridge(
  overrides: Partial<ConstructorParameters<typeof BridgeServer>[0]> & { gateway?: FakeGateway } = {},
): Promise<Harness> {
  const invokeRpc = vi.fn(async (): Promise<LegacyResult> => ({ ok: true, value: 'ok' }))
  const gateway = overrides.gateway ?? fakeGateway()
  const bridge = new BridgeServer({
    token: TOKEN,
    invokeRpc: invokeRpc as unknown as ConstructorParameters<typeof BridgeServer>[0]['invokeRpc'],
    createEventFeed: (sink, onError) => new BridgeEventFeed(gateway, sink, onError),
    toolTimeoutMs: 1_000,
    caps: { textOnly: true, snapshotMaxChars: 12_000, maxInteractiveItems: 60 },
    injectBrowserSnapshot: vi.fn(),
    purgeSession: vi.fn(async () => {}),
    ...overrides,
  })
  const server = createServer()
  server.on('upgrade', (req, socket, head) => { bridge.handleUpgrade(req, socket, head) })
  await new Promise<void>((resolve) => { server.listen(0, '127.0.0.1', resolve) })
  const port = (server.address() as AddressInfo).port
  return { bridge, server, url: `ws://127.0.0.1:${port}/ext/bridge`, invokeRpc, gateway }
}

function connect(url: string, origin?: string): Promise<{ ws: WebSocket; frames: BridgeFrame[]; done: Promise<void> }> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, origin !== undefined ? { headers: { origin } } : undefined)
    const frames: BridgeFrame[] = []
    ws.on('message', (data) => { frames.push(JSON.parse(data.toString()) as BridgeFrame) })
    ws.on('error', reject)
    ws.on('open', () => {
      resolve({
        ws,
        frames,
        done: new Promise<void>((doneResolve) => {
          ws.on('close', () => { doneResolve() })
        }),
      })
    })
  })
}

function send(ws: WebSocket, frame: BridgeFrame): void {
  ws.send(JSON.stringify(frame))
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error('waitFor timed out')
    await new Promise((resolve) => { setTimeout(resolve, 10) })
  }
}

const harnesses: Harness[] = []
afterEach(async () => {
  for (const h of harnesses.splice(0)) {
    await h.bridge.close()
    await new Promise<void>((resolve) => { h.server.close(() => resolve()) })
  }
})

describe('BridgeServer', () => {
  it('decodes every ws message delivery shape', () => {
    expect(messageToText([Buffer.from('a'), Buffer.from('b')])).toBe('ab')
    expect(messageToText(Buffer.from('hi'))).toBe('hi')
    expect(messageToText(new TextEncoder().encode('x').buffer)).toBe('x')
  })

  it('extracts tool error codes and messages with parser-gated fallbacks', () => {
    expect(payloadCode({ code: 'timeout', message: 'm' })).toBe('timeout')
    expect(payloadCode({ code: 42, message: 'm' })).toBe('internal')
    expect(payloadCode('garbage')).toBe('internal')
    expect(payloadCode(null)).toBe('internal')
    expect(payloadMessage({ code: 'x', message: 'm' })).toBe('m')
    expect(payloadMessage({ code: 'x', message: '' })).toBe('browser action failed')
    expect(payloadMessage({ code: 'x', message: 42 })).toBe('browser action failed')
    expect(payloadMessage('garbage')).toBe('browser action failed')
  })

  it('authenticates a valid hello and acknowledges caps', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: { textOnly: true, snapshotMaxChars: 12000, maxInteractiveItems: 60 } })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    expect(frames.find((f) => f.t === 'hello.ok')).toEqual({ t: 'hello.ok', caps: { textOnly: true, snapshotMaxChars: 12000, maxInteractiveItems: 60 } })
    ws.close()
  })

  it('accepts loopback connections without a token when Origin is an extension (zero-config mode)', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url, EXT_ORIGIN)
    send(ws, { t: 'hello', token: '', caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    expect(frames.find((f) => f.t === 'hello.ok')).toBeDefined()
    ws.close()
  })

  it('requires a token from Firefox extension origins because their UUID is not an extension identity', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, done } = await connect(h.url, FIREFOX_EXT_ORIGIN)
    send(ws, { t: 'hello', token: '', caps: CAPS })
    await done
    expect(ws.readyState).toBe(WebSocket.CLOSED)
  })

  it('accepts an authenticated Firefox extension origin', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url, FIREFOX_EXT_ORIGIN)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    expect(frames.find((f) => f.t === 'hello.ok')).toBeDefined()
    ws.close()
  })

  it('rejects loopback connections without a token when Origin is not an extension (malicious page)', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, done } = await connect(h.url, 'https://evil.example')
    send(ws, { t: 'hello', token: '', caps: CAPS })
    await done
    expect(ws.readyState).toBe(WebSocket.CLOSED)
  })

  it('rejects loopback connections without a token and without any Origin', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, done } = await connect(h.url)
    send(ws, { t: 'hello', token: '', caps: CAPS })
    await done
    expect(ws.readyState).toBe(WebSocket.CLOSED)
  })

  it('still requires the token from non-loopback remotes', async () => {
    const h = await startBridge({ remoteAddressOverride: '192.168.1.5' })
    harnesses.push(h)
    const { ws, done } = await connect(h.url)
    send(ws, { t: 'hello', token: '', caps: CAPS })
    await done
    expect(ws.readyState).toBe(WebSocket.CLOSED)
  })

  it('closes sockets that never present hello', async () => {
    const h = await startBridge({ helloTimeoutMs: 500 })
    harnesses.push(h)
    const { ws, done } = await connect(h.url)
    await done
    expect(ws.readyState).toBe(WebSocket.CLOSED)
  })

  it('rejects frames before hello', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, done } = await connect(h.url)
    send(ws, { t: 'pong' })
    await done
    expect(ws.readyState).toBe(WebSocket.CLOSED)
  })

  it('passes rpc frames to the legacy invoker and relays the envelope', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: { textOnly: true, snapshotMaxChars: 12000, maxInteractiveItems: 60 } })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    send(ws, { t: 'rpc', id: 'rpc-1', method: 'session.list', payload: {} })
    await waitFor(() => frames.some((f) => f.t === 'rpc.result'))
    const result = frames.find((f) => f.t === 'rpc.result')
    expect(result).toMatchObject({ t: 'rpc.result', id: 'rpc-1', ok: true })
    expect(h.invokeRpc).toHaveBeenCalledTimes(1)
    expect(h.invokeRpc.mock.calls[0]![0]).toBe('session.list')
    expect(h.invokeRpc.mock.calls[0]![1]).toEqual({})
    expect(result).toMatchObject({
      t: 'rpc.result',
      id: 'rpc-1',
      ok: true,
      result: { type: 'server-response', rpcId: 'rpc-1', result: { ok: true, value: 'ok' } },
    })
    ws.close()
  })

  it('reports invoker throws as rpc.result errors', async () => {
    const h = await startBridge()
    harnesses.push(h)
    h.invokeRpc.mockRejectedValueOnce(new Error('boom'))
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: { textOnly: true, snapshotMaxChars: 12000, maxInteractiveItems: 60 } })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    send(ws, { t: 'rpc', id: 'rpc-2', method: 'session.list', payload: {} })
    await waitFor(() => frames.some((f) => f.t === 'rpc.result' && f.id === 'rpc-2'))
    expect(frames.find((f) => f.t === 'rpc.result' && f.id === 'rpc-2'))
      .toMatchObject({ t: 'rpc.result', id: 'rpc-2', ok: false, error: { code: 'internal', message: 'Error: boom' } })
    ws.close()
  })

  it('injects followed-page snapshots without forwarding the internal RPC to the invoker', async () => {
    const injectBrowserSnapshot = vi.fn()
    const h = await startBridge({ injectBrowserSnapshot })
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((frame) => frame.t === 'hello.ok'))

    send(ws, {
      t: 'rpc',
      id: 'snapshot-1',
      method: BRIDGE_INJECT_BROWSER_SNAPSHOT_METHOD,
      payload: { sessionId: 'session-1', snapshot: 'Page: Other target' },
    })
    await waitFor(() => frames.some((frame) => frame.t === 'rpc.result' && frame.id === 'snapshot-1'))

    expect(injectBrowserSnapshot).toHaveBeenCalledWith('session-1', 'Page: Other target')
    expect(h.invokeRpc).not.toHaveBeenCalled()
    expect(frames).toContainEqual({
      t: 'rpc.result', id: 'snapshot-1', ok: true, result: { accepted: true },
    })

    send(ws, {
      t: 'rpc',
      id: 'snapshot-invalid',
      method: BRIDGE_INJECT_BROWSER_SNAPSHOT_METHOD,
      payload: { sessionId: '', snapshot: '' },
    })
    await waitFor(() => frames.some((frame) => frame.t === 'rpc.result' && frame.id === 'snapshot-invalid'))
    expect(frames).toContainEqual(expect.objectContaining({
      t: 'rpc.result', id: 'snapshot-invalid', ok: false, error: expect.objectContaining({ code: 'bad-request' }),
    }))
    ws.close()
  })

  it('purges sessions through the internal RPC without forwarding it to the invoker', async () => {
    const purgeSession = vi.fn(async () => {})
    const h = await startBridge({ purgeSession })
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((frame) => frame.t === 'hello.ok'))

    send(ws, {
      t: 'rpc',
      id: 'purge-1',
      method: BRIDGE_SESSION_PURGE_METHOD,
      payload: { sessionId: 'session-82222a77-aab5-4c0b-b33e-6376973ec93d' },
    })
    await waitFor(() => frames.some((frame) => frame.t === 'rpc.result' && frame.id === 'purge-1'))

    expect(purgeSession).toHaveBeenCalledWith('session-82222a77-aab5-4c0b-b33e-6376973ec93d')
    expect(h.invokeRpc).not.toHaveBeenCalled()
    expect(frames).toContainEqual({
      t: 'rpc.result', id: 'purge-1', ok: true, result: { purged: true },
    })

    send(ws, {
      t: 'rpc',
      id: 'purge-invalid',
      method: BRIDGE_SESSION_PURGE_METHOD,
      payload: { sessionId: '' },
    })
    await waitFor(() => frames.some((frame) => frame.t === 'rpc.result' && frame.id === 'purge-invalid'))
    expect(frames).toContainEqual(expect.objectContaining({
      t: 'rpc.result', id: 'purge-invalid', ok: false, error: expect.objectContaining({ code: 'bad-request' }),
    }))

    const failing = vi.fn(async () => { throw new SessionPurgeError('running', 'cancel it first') })
    const failureBridge = await startBridge({ purgeSession: failing })
    harnesses.push(failureBridge)
    const failure = await connect(failureBridge.url)
    send(failure.ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => failure.frames.some((frame) => frame.t === 'hello.ok'))
    send(failure.ws, {
      t: 'rpc',
      id: 'purge-running',
      method: BRIDGE_SESSION_PURGE_METHOD,
      payload: { sessionId: 'session-82222a77-aab5-4c0b-b33e-6376973ec93d' },
    })
    await waitFor(() => failure.frames.some((frame) => frame.t === 'rpc.result' && frame.id === 'purge-running'))
    expect(failure.frames).toContainEqual(expect.objectContaining({
      t: 'rpc.result', id: 'purge-running', ok: false, error: { code: 'running', message: 'cancel it first' },
    }))
    ws.close()
    failure.ws.close()
  })

  it('finishes snapshot injection before forwarding a prompt for the same session', async () => {
    let releaseInjection!: () => void
    const injectionGate = new Promise<void>((resolve) => { releaseInjection = resolve })
    const injectBrowserSnapshot = vi.fn(async () => { await injectionGate })
    const h = await startBridge({ injectBrowserSnapshot })
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((frame) => frame.t === 'hello.ok'))

    send(ws, {
      t: 'rpc', id: 'snapshot', method: BRIDGE_INJECT_BROWSER_SNAPSHOT_METHOD,
      payload: { sessionId: 'session-ordered', snapshot: 'Current page' },
    })
    send(ws, {
      t: 'rpc', id: 'prompt-after-snapshot', method: 'session.prompt',
      payload: { sessionId: 'session-ordered', mode: 'queue', content: [] },
    })

    await waitFor(() => injectBrowserSnapshot.mock.calls.length === 1)
    expect(h.invokeRpc).not.toHaveBeenCalled()
    releaseInjection()
    await waitFor(() => h.invokeRpc.mock.calls.length === 1)
    await waitFor(() => frames.some((frame) => frame.t === 'rpc.result' && frame.id === 'prompt-after-snapshot'))
    expect(frames.filter((frame) => frame.t === 'rpc.result').map((frame) => frame.id)).toEqual([
      'snapshot',
      'prompt-after-snapshot',
    ])
    ws.close()
  })

  it('orders prompt before cancel for one session without blocking other sessions', async () => {
    let releasePrompt!: () => void
    const promptGate = new Promise<void>((resolve) => { releasePrompt = resolve })
    const calls: Array<{ method: string; sessionId: string }> = []
    const invokeRpc = vi.fn(async (method: string, payload: unknown): Promise<LegacyResult> => {
      const { sessionId } = payload as { sessionId: string }
      calls.push({ method, sessionId })
      if (method === 'session.prompt' && sessionId === 'provisional') await promptGate
      return { ok: true, value: { accepted: true } }
    })
    const h = await startBridge({ invokeRpc })
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((frame) => frame.t === 'hello.ok'))

    send(ws, {
      t: 'rpc', id: 'prompt', method: 'session.prompt',
      payload: { sessionId: 'provisional', mode: 'queue', content: [] },
    })
    send(ws, { t: 'rpc', id: 'cancel', method: 'session.cancel', payload: { sessionId: 'provisional' } })
    send(ws, { t: 'rpc', id: 'other-cancel', method: 'session.cancel', payload: { sessionId: 'other' } })

    await waitFor(() => calls.some((call) => call.sessionId === 'other'))
    expect(calls).toContainEqual({ method: 'session.prompt', sessionId: 'provisional' })
    expect(calls).toContainEqual({ method: 'session.cancel', sessionId: 'other' })
    expect(calls).not.toContainEqual({ method: 'session.cancel', sessionId: 'provisional' })

    releasePrompt()
    await waitFor(() => calls.some((call) => call.method === 'session.cancel' && call.sessionId === 'provisional'))
    expect(calls.filter((call) => call.sessionId === 'provisional')).toEqual([
      { method: 'session.prompt', sessionId: 'provisional' },
      { method: 'session.cancel', sessionId: 'provisional' },
    ])
    await waitFor(() => frames.filter((frame) => frame.t === 'rpc.result').length === 3)
    ws.close()
  })

  it('answers pending waterfalls through the Typert $events/result carrier', async () => {
    const gateway = fakeGateway({
      '$events': async function* () {
        yield { type: 'ready', clientId: 'client-1' }
        yield {
          type: 'waterfall',
          event: 'user-questions/request',
          eventId: 'question-1',
          agentId: 'session-1',
          request: { questions: [{ id: 'db', question: 'SQLite or Mongo?' }] },
        }
      },
    })
    const h = await startBridge({ gateway })
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((frame) => frame.t === 'hello.ok'))
    await waitFor(() => frames.some((frame) => frame.t === 'event' && frame.frame.method === 'question/requested'))

    send(ws, {
      t: 'respond',
      id: 'response-1',
      rpcId: 'question-1',
      result: { ok: true, value: { sessionId: 'session-1', answer: { answers: [{ id: 'db', selected: ['SQLite'] }] } } },
    })
    await waitFor(() => frames.some((frame) => frame.t === 'respond.result'))

    expect(frames).toContainEqual({
      t: 'respond.result', id: 'response-1', ok: true, result: { accepted: true },
    })
    expect(gateway.dispatchRpc).toHaveBeenCalledWith('$events/result', {
      args: {
        clientId: 'client-1',
        eventId: 'question-1',
        outcome: { kind: 'result', value: { answers: [{ id: 'db', selected: ['SQLite'] }] } },
      },
    })
    ws.close()
  })

  it('translates dismissed questions into ASK_CANCELLED rejections', async () => {
    const gateway = fakeGateway({
      '$events': async function* () {
        yield { type: 'ready', clientId: 'client-2' }
        yield {
          type: 'waterfall',
          event: 'user-questions/request',
          eventId: 'question-2',
          agentId: 'session-2',
          request: { questions: [{ id: 'q', question: 'Pick one' }] },
        }
      },
    })
    const h = await startBridge({ gateway })
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((frame) => frame.t === 'hello.ok'))
    await waitFor(() => frames.some((frame) => frame.t === 'event' && frame.frame.method === 'question/requested'))

    send(ws, {
      t: 'respond', id: 'response-2', rpcId: 'question-2',
      result: { ok: false, error: { code: 'cancelled', message: 'user dismissed the question', details: {} } },
    })
    await waitFor(() => frames.some((frame) => frame.t === 'respond.result' && frame.id === 'response-2'))
    expect(gateway.dispatchRpc).toHaveBeenCalledWith('$events/result', {
      args: {
        clientId: 'client-2',
        eventId: 'question-2',
        outcome: {
          kind: 'rejected',
          error: { name: 'UserQuestionError', message: 'user dismissed the question', code: 'ASK_CANCELLED' },
        },
      },
    })

    // Unknown error codes keep the old bad-response refusal.
    send(ws, {
      t: 'respond', id: 'response-3', rpcId: 'question-2',
      result: { ok: false, error: { code: 'bogus', message: 'nope', details: {} } },
    })
    await waitFor(() => frames.some((frame) => frame.t === 'respond.result' && frame.id === 'response-3'))
    expect(frames).toContainEqual({
      t: 'respond.result', id: 'response-3', ok: true, result: { accepted: false, reason: 'bad-response' },
    })
    ws.close()
  })

  it('reports not-pending answers for unknown waterfalls', async () => {
    const gateway = fakeGateway({ '$events': async function* () { yield { type: 'ready', clientId: 'c' } } })
    const h = await startBridge({ gateway })
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((frame) => frame.t === 'hello.ok'))
    send(ws, {
      t: 'respond', id: 'response-4', rpcId: 'unknown',
      result: { ok: true, value: { sessionId: 's', answer: { answers: [] } } },
    })
    await waitFor(() => frames.some((frame) => frame.t === 'respond.result' && frame.id === 'response-4'))
    expect(frames).toContainEqual({
      t: 'respond.result', id: 'response-4', ok: true, result: { accepted: false, reason: 'not-pending' },
    })
    ws.close()
  })

  it('rejects privileged methods from non-loopback remotes', async () => {
    expect(isLoopbackAddress('127.0.0.1')).toBe(true)
    expect(isLoopbackAddress('::1')).toBe(true)
    expect(isLoopbackAddress('::ffff:127.0.0.1')).toBe(true)
    expect(isLoopbackAddress('192.168.1.5')).toBe(false)
    expect(isLoopbackAddress(undefined)).toBe(false)
  })

  it('dispatches tool calls and resolves on tool.result', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: { textOnly: true, snapshotMaxChars: 12000, maxInteractiveItems: 60 } })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    const result = h.bridge.requestTool('browser_click', { index: 1 }, new AbortController().signal)
    await waitFor(() => frames.some((f) => f.t === 'tool.call'))
    const call = frames.find((f) => f.t === 'tool.call') as Extract<BridgeFrame, { t: 'tool.call' }>
    expect(call.name).toBe('browser_click')
    expect(call.args).toEqual({ index: 1 })
    send(ws, { t: 'tool.result', id: call.id, ok: true, result: { text: 'clicked' } })
    await expect(result).resolves.toEqual({ text: 'clicked' })
    ws.close()
  })

  it('rejects tool calls whose signal is already aborted before dispatch', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url, EXT_ORIGIN)
    send(ws, { t: 'hello', token: '', caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    const abort = new AbortController()
    abort.abort()
    expect(() => h.bridge.requestTool('browser_click', {}, abort.signal))
      .toThrowError(expect.objectContaining({ code: 'bridge-closed' }))
    // 没有 tool.call 被发出
    await new Promise((resolve) => { setTimeout(resolve, 50) })
    expect(frames.some((f) => f.t === 'tool.call')).toBe(false)
    ws.close()
  })

  it('rejects tool calls when no extension is connected', async () => {
    const h = await startBridge()
    harnesses.push(h)
    expect(() => h.bridge.requestTool('browser_click', {}, new AbortController().signal))
      .toThrowError(expect.objectContaining({ code: 'bridge-closed' }))
  })

  it('times out tool calls that never settle', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: { textOnly: true, snapshotMaxChars: 12000, maxInteractiveItems: 60 } })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    await expect(h.bridge.requestTool('browser_wait', {}, new AbortController().signal, 30))
      .rejects.toMatchObject({ code: 'timeout' })
    await waitFor(() => frames.some((frame) => frame.t === 'tool.cancel'))
    const call = frames.find((frame) => frame.t === 'tool.call') as Extract<BridgeFrame, { t: 'tool.call' }>
    expect(frames).toContainEqual({ t: 'tool.cancel', id: call.id })
    ws.close()
  })

  it('propagates extension-reported tool errors', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: { textOnly: true, snapshotMaxChars: 12000, maxInteractiveItems: 60 } })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    const result = h.bridge.requestTool('browser_navigate', { url: 'https://x' }, new AbortController().signal)
    await waitFor(() => frames.some((f) => f.t === 'tool.call'))
    const call = frames.find((f) => f.t === 'tool.call') as Extract<BridgeFrame, { t: 'tool.call' }>
    send(ws, { t: 'tool.result', id: call.id, ok: false, error: { code: 'action-failed', message: 'blocked' } })
    await expect(result).rejects.toBeInstanceOf(BridgeToolError)
    await expect(result).rejects.toMatchObject({ code: 'action-failed', message: 'blocked' })
    ws.close()
  })

  it('settles pending tool calls when a replacement connection arrives', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const first = await connect(h.url)
    send(first.ws, { t: 'hello', token: TOKEN, caps: { textOnly: true, snapshotMaxChars: 12000, maxInteractiveItems: 60 } })
    await waitFor(() => first.frames.some((f) => f.t === 'hello.ok'))
    const pending = h.bridge.requestTool('browser_click', {}, new AbortController().signal)
    // Attach the assertion eagerly: the replacement below settles it before the final await.
    const pendingAssertion = expect(pending).rejects.toMatchObject({ code: 'bridge-closed' })
    await waitFor(() => first.frames.some((f) => f.t === 'tool.call'))

    const second = await connect(h.url)
    send(second.ws, { t: 'hello', token: TOKEN, caps: { textOnly: true, snapshotMaxChars: 12000, maxInteractiveItems: 60 } })
    await waitFor(() => second.frames.some((f) => f.t === 'hello.ok'))

    await pendingAssertion
    first.ws.close()
    second.ws.close()
  })

  it('aborts tool calls when the caller signal fires', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: { textOnly: true, snapshotMaxChars: 12000, maxInteractiveItems: 60 } })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    const abort = new AbortController()
    const pending = h.bridge.requestTool('browser_click', {}, abort.signal)
    await waitFor(() => frames.some((f) => f.t === 'tool.call'))
    const call = frames.find((frame) => frame.t === 'tool.call') as Extract<BridgeFrame, { t: 'tool.call' }>
    abort.abort()
    await expect(pending).rejects.toMatchObject({ code: 'bridge-closed' })
    await waitFor(() => frames.some((frame) => frame.t === 'tool.cancel'))
    expect(frames).toContainEqual({ t: 'tool.cancel', id: call.id })
    ws.close()
  })

  it('forwards the owning session with a tool call', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    const pending = h.bridge.requestTool(
      'browser_click',
      {},
      new AbortController().signal,
      1_000,
      'session-browser',
    )
    await waitFor(() => frames.some((f) => f.t === 'tool.call'))
    const call = frames.find((frame) => frame.t === 'tool.call') as Extract<BridgeFrame, { t: 'tool.call' }>
    expect(call.sessionId).toBe('session-browser')
    send(ws, { t: 'tool.result', id: call.id, ok: true, result: { text: 'done' } })
    await expect(pending).resolves.toEqual({ text: 'done' })
    ws.close()
  })

  it('pumps waterfall and question frames to the connected extension', async () => {
    const gateway = fakeGateway({
      '$events': async function* () {
        yield { type: 'ready', clientId: 'client-3' }
        yield {
          type: 'waterfall',
          event: 'user-questions/request',
          eventId: 'e1',
          agentId: 's1',
          request: { questions: [{ id: 'q', question: 'Pick one' }] },
        }
        yield { type: 'cancel', eventId: 'e1' }
      },
    })
    const h = await startBridge({ gateway })
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: { textOnly: true, snapshotMaxChars: 12000, maxInteractiveItems: 60 } })
    await waitFor(() => frames.filter((f) => f.t === 'event').length >= 2)
    const events = frames.filter((f) => f.t === 'event') as Extract<BridgeFrame, { t: 'event' }>[]
    expect(events.map((e) => e.frame.method)).toEqual(['question/requested', 'question/resolved'])
    expect(events[0]!.frame).toMatchObject({
      rpcId: 'e1',
      payload: { sessionId: 's1', questions: [{ id: 'q', question: 'Pick one' }] },
    })
    expect(events[1]!.frame).toMatchObject({
      rpcId: 'e1',
      payload: { sessionId: 's1', questionRpcId: 'e1' },
    })
    ws.close()
  })

  it('settles pending tool calls when the send fails mid-flight', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    const pending = h.bridge.requestTool('browser_click', {}, new AbortController().signal)
    // Tear the socket down immediately: the in-flight send reports a write
    // failure (or the close path wins — either settles as bridge-closed).
    const assertion = expect(pending).rejects.toMatchObject({ code: 'bridge-closed' })
    ws.terminate()
    await assertion
  })

  it('closes cleanly twice (second close is a no-op on the acceptor)', async () => {
    const h = await startBridge()
    harnesses.push(h)
    await h.bridge.close()
    await h.bridge.close()
  })

  it('sends protocol pings on the configured cadence and the client answers pong', async () => {
    const h = await startBridge({ pingIntervalMs: 50 })
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'ping'))
    send(ws, { t: 'pong' })
    ws.close()
  })

  it('emits a stream-failed error frame when the $events stream throws', async () => {
    const gateway = fakeGateway({
      '$events': async function* () {
        yield { type: 'ready', clientId: 'c' }
        throw new Error('stream broke')
      },
    })
    const h = await startBridge({ gateway })
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'error' && f.code === 'stream-failed'))
    expect(frames.find((f) => f.t === 'error')).toMatchObject({ t: 'error', code: 'stream-failed' })
    ws.close()
  })

  it('stops pumping events once the socket closes mid-stream', async () => {
    const gateway = fakeGateway({
      '$events': async function* () {
        yield { type: 'ready', clientId: 'c' }
        for (let i = 0; i < 100; i += 1) {
          yield {
            type: 'waterfall',
            event: 'user-questions/request',
            eventId: `q${i}`,
            agentId: 's1',
            request: { questions: [{ id: `q${i}`, question: 'Pick one' }] },
          }
          await new Promise((resolve) => { setTimeout(resolve, 10) })
        }
      },
    })
    const h = await startBridge({ gateway })
    harnesses.push(h)
    const { ws, frames, done } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.filter((f) => f.t === 'event').length >= 2)
    ws.close()
    await done
    // The pump must stop sending after close instead of writing to a dead socket.
    const countBefore = frames.filter((f) => f.t === 'event').length
    await new Promise((resolve) => { setTimeout(resolve, 80) })
    expect(frames.filter((f) => f.t === 'event').length).toBe(countBefore)
  })

  it('closes cleanly and rejects pending work', async () => {
    const h = await startBridge()
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    const pending = h.bridge.requestTool('browser_click', {}, new AbortController().signal)
    // Attach the assertion eagerly: close() settles it before the final await.
    const pendingAssertion = expect(pending).rejects.toMatchObject({ code: 'bridge-closed' })
    await h.bridge.close()
    await pendingAssertion
    expect(() => h.bridge.requestTool('browser_click', {}, new AbortController().signal))
      .toThrowError(expect.objectContaining({ code: 'bridge-closed' }))
    ws.close()
  })

  it('tracks connection state through auth, close, and replacement', async () => {
    const h = await startBridge()
    harnesses.push(h)
    expect(h.bridge.hasConnection()).toBe(false)
    const { ws, frames, done } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    expect(h.bridge.hasConnection()).toBe(true)
    ws.close()
    await done
    // The server processes the close asynchronously; poll for the outcome.
    await expect.poll(() => h.bridge.hasConnection()).toBe(false)
  })

  it('closes sockets on unparseable frames and ignores client-only frames when ready', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames, done } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    // Client-only shapes after ready are ignored (no error frame, no close).
    send(ws, { t: 'pong' })
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await new Promise((resolve) => { setTimeout(resolve, 50) })
    expect(ws.readyState).toBe(WebSocket.OPEN)
    // Garbage is a protocol violation and closes the socket.
    ws.send('not-json')
    await done
    expect(ws.readyState).toBe(WebSocket.CLOSED)
  })

  it('ignores tool results with unknown ids', async () => {
    const h = await startBridge()
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    // Unknown id: ignored, connection stays healthy.
    send(ws, { t: 'tool.result', id: 'nope', ok: true, result: {} })
    await new Promise((resolve) => { setTimeout(resolve, 50) })
    expect(ws.readyState).toBe(WebSocket.OPEN)
    ws.close()
  })

  it('rejects privileged methods from non-loopback remotes over a real socket', async () => {
    // The sandbox cannot bind arbitrary loopback literals, so the remote
    // address is forced through the test seam; the socket itself is real.
    const h = await startBridge({ remoteAddressOverride: '192.168.1.5' })
    harnesses.push(h)
    const { ws, frames } = await connect(h.url)
    send(ws, { t: 'hello', token: TOKEN, caps: CAPS })
    await waitFor(() => frames.some((f) => f.t === 'hello.ok'))
    send(ws, { t: 'rpc', id: 'priv-1', method: 'settings.describe', payload: {} })
    await waitFor(() => frames.some((f) => f.t === 'rpc.result' && f.id === 'priv-1'))
    expect(frames.find((f) => f.t === 'rpc.result' && f.id === 'priv-1'))
      .toMatchObject({ t: 'rpc.result', id: 'priv-1', ok: false, error: { code: 'forbidden' } })
    // Non-privileged methods still pass for the same remote.
    send(ws, { t: 'rpc', id: 'priv-2', method: 'session.list', payload: {} })
    await waitFor(() => frames.some((f) => f.t === 'rpc.result' && f.id === 'priv-2'))
    const allowed = frames.find((f): f is Extract<BridgeFrame, { t: 'rpc.result' }> => f.t === 'rpc.result' && f.id === 'priv-2')!
    expect(allowed.ok).toBe(true)
    ws.close()
  })
})
