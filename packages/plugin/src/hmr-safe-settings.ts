/**
 * Run settings writes from the context in which the plugin module was first
 * loaded. DSH's HMR AsyncLocalStorage marker can leak into a reloaded fiber;
 * configEditor then sees every later write as a nested transaction. The pump
 * is kept on globalThis so a hot reload reuses the original clean context.
 */

type Job<T> = {
  run: () => Promise<T>
  resolve: (value: T | PromiseLike<T>) => void
  reject: (reason?: unknown) => void
}

interface Pump {
  enqueue<T>(run: () => Promise<T>): Promise<T>
}

const PUMP_KEY = Symbol.for('ds-harness-remote.hmr-safe-settings-pump')

function createPump(): Pump {
  const queue: Job<unknown>[] = []
  let wake!: () => void
  let sleeping = new Promise<void>(resolve => { wake = resolve })

  // Do not await this from plugin activation. Its continuation is deliberately
  // left at module initialization, outside the HMR transaction that may reload
  // the plugin later.
  void (async () => {
    for (;;) {
      if (queue.length === 0) {
        await sleeping
        sleeping = new Promise<void>(resolve => { wake = resolve })
      }
      const job = queue.shift()
      if (job === undefined) continue
      try {
        job.resolve(await job.run())
      } catch (error) {
        job.reject(error)
      }
    }
  })()

  return {
    enqueue<T>(run: () => Promise<T>): Promise<T> {
      return new Promise<T>((resolve, reject) => {
        queue.push({ run, resolve: resolve as (value: unknown) => void, reject })
        wake()
      })
    },
  }
}

const globalState = globalThis as typeof globalThis & { [PUMP_KEY]?: Pump }
const pump = globalState[PUMP_KEY] ?? (globalState[PUMP_KEY] = createPump())

export function runHmrSafe<T>(run: () => Promise<T>): Promise<T> {
  return pump.enqueue(run)
}
