import { RedisConnection, Worker, WorkerOptions, Job } from 'bullmq'
import type { DefaultJob, ConnectionStatus } from './QueueManager'

export type WorkerConfig = Partial<WorkerOptions> | boolean | undefined | null
export type Workers<QN extends string> = Record<QN, WorkerConfig>
export type WorkerManagerOptions = {}

export class WorkerManager<
  JNs extends string,
  QNs extends string,
  J extends DefaultJob<JNs>,
> {
  protected workers = {} as Record<QNs, Worker<any, any, JNs>>
  protected connectionStatus: ConnectionStatus = 'disconnected'

  constructor(
    workersConfig: Workers<QNs>,
    processor: (job: Job<any, any, JNs>) => Promise<unknown>,
    workerOptions: WorkerOptions,
    protected options: WorkerManagerOptions,
    Connection?: typeof RedisConnection,
  ) {
    const configIterator = Object.entries<WorkerConfig>(workersConfig)

    for (const [name, workerConfig] of configIterator) {
      if (workerConfig) {
        const worker = new Worker(
          name,
          processor,
          {
            ...workerOptions,
            ...(workerConfig === true ? undefined : workerConfig),
          },
          Connection
        )

        this.workers[name as QNs] = worker
      }
    }
  }

  run() {
    if (this.connectionStatus !== 'connected') {
      this.connectionStatus = 'connected'
    } else {
      console.log(`${this.constructor.name} is already running`)
      return
    }
    for (const w of this.getWorkers()) {
      w.run()
    }
  }

  async waitUntilReady() {
    if (this.connectionStatus !== 'connected') {
      this.connectionStatus = 'connected'
    } else {
      console.log(`${this.constructor.name} is already running`)
      return
    }

    await Promise.all(
      this.getWorkers().map((q) => q.waitUntilReady())
    )
  }

  async close() {
    this.checkConnectionStatus()
    this.connectionStatus = 'closed'

    await Promise.all(
      this.getWorkers().map((w) => w.close())
    )
  }

  getWorker(name: QNs) {
    return this.workers[name]
  }

  getWorkers() {
    return Object.values<Worker<any, any, JNs>>(this.workers)
  }

  private checkConnectionStatus() {
    if (this.connectionStatus === 'disconnected') {
      throw new Error('WorkerManager is disconnected')
    } else if (this.connectionStatus === 'closed') {
      throw new Error('WorkerManager is closed')
    }
  }
}
