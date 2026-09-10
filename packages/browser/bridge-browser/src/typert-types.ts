/**
 * Structural mirror of the dsh 0.1.5 Typert Gateway surface.
 *
 * The gateway package is only published bundled inside `@deepseek-ai/dsh`
 * (npm carries no standalone 0.1.5-rc.1 release), so the bridge declares the
 * seam it consumes instead of depending on the package. Cordis injects the
 * real service at runtime by key; these shapes are compile-time only.
 *
 * @module @yuxianglin/dsh-bridge-browser/src/typert-types
 */

/** One Remote method request after a carrier has decoded its envelope. */
export interface InvokeRemoteRequestLike {
  /** Remote namespace selected by the generated descriptor. */
  readonly namespace: string
  /** Exported Service method name. */
  readonly method: string
  /** Named wire values; fields must exactly match the descriptor. */
  readonly args: Readonly<Record<string, unknown>>
  /** Carrier or direct-caller cancellation injected only into cancellation-aware methods. */
  readonly signal?: AbortSignal
}

/** Carrier-facing access to decoded Remote streams and their stable failures. */
export interface TypertGatewayWireStreamLike {
  readonly open: (endpoint: string, payload: unknown, signal: AbortSignal) => Promise<AsyncIterable<unknown>>
  readonly failure: (error: unknown) => {
    readonly code: string
    readonly message: string
    readonly details: object
  }
}

/** Host dispatcher for Typert Remote calls (`ctx.typertGateway`). */
export interface TypertGatewayLike {
  readonly wireStream: TypertGatewayWireStreamLike
  /** Invoke one live Remote method without assuming a carrier or response envelope. */
  invoke(request: InvokeRemoteRequestLike): Promise<unknown>
  /** Open one live stream Remote method without assuming a physical carrier. */
  stream(request: InvokeRemoteRequestLike): Promise<AsyncIterable<unknown>>
}

declare module '@deepseek-ai/cordis' {
  interface Context {
    /** Host dispatcher for Typert Remote calls. */
    typertGateway: TypertGatewayLike
  }
}
