import { Worker } from 'bullmq'
import type { WorkerOptions, Job, IQueueBackend } from 'bullmq'
import type { DefaultJob, ConnectionStatus } from './QueueManager'

export type ManagedWorker<JN extends string> = Worker<any, any, JN, IQueueBackend>

export type WorkerConfig = Partial<WorkerOptions> | boolean | undefined | null
export type Workers<QN extends string> = Record<QN, WorkerConfig>
export type WorkerManagerOptions = {}

export class WorkerManager<
  JNs extends string,
  QNs extends string,
  J extends DefaultJob<JNs>,
> {
  protected workers = {} as Record<QNs, ManagedWorker<JNs>>

  constructor(
    workersConfig: Workers<QNs>,
    processor: (job: Job<any, any, JNs>) => Promise<unknown>,
    workerOptions: WorkerOptions,
    protected options: WorkerManagerOptions = {},
  ) {
    const configIterator = Object.entries<WorkerConfig>(workersConfig)

    for (const [name, workerConfig] of configIterator) {
      if (workerConfig) {
        const worker = new Worker<any, any, JNs, IQueueBackend>(
          name,
          processor,
          {
            ...workerOptions,
            ...(workerConfig === true ? undefined : workerConfig),
          },
        )

        this.workers[name as QNs] = worker
      }
    }
  }

  run() {
    for (const w of this.getWorkers()) {
      if (!w.isRunning()) {
        w.run().catch((err) => {
          // run() rejects only on fatal startup errors; route them to the worker's error listeners
          w.emit('error', err)
        })
      }
    }
  }

  async waitUntilReady() {
    await Promise.all(
      this.getWorkers().map((q) => q.waitUntilReady())
    )
  }

  async close() {
    await Promise.all(
      this.getWorkers().map((w) => w.close())
    )
  }

  getWorker(name: QNs) {
    return this.workers[name]
  }

  getWorkers() {
    return Object.values<ManagedWorker<JNs>>(this.workers)
  }
}
