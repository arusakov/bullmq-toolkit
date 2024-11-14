import { Queue, QueueListener } from 'bullmq'
import type { QueueOptions, RedisConnection, DefaultJobOptions } from 'bullmq'

export type Queues<QN extends string> = Record<QN, QueueOptions | boolean | undefined | null>
export type NameToQueue<JN extends string, QN extends string> = Record<JN, QN>
export type DefaultJob<JN extends string> = {
  name: JN
  data: unknown
  opts?: DefaultJobOptions
}

export type ConnectionStatus = 'connected' | 'disconnected' | 'closed'

export type FlowJob<JN extends string> = DefaultJob<JN> & {
  children?: Array<FlowJob<JN>>
}

export type Options = {}

export class QueueManager<
  JNs extends string,
  QNs extends string,
  J extends DefaultJob<JNs>,
> {
  protected queues = {} as Record<QNs, Queue>
  protected connectionStatus: ConnectionStatus = 'disconnected'

  constructor(
    queues: Queues<QNs>,
    queueOptions: QueueOptions,
    protected nameToQueue: NameToQueue<JNs, QNs>,
    protected options: Options = {},
    Connection?: typeof RedisConnection,
  ) {
    const qIterator = Object.entries<QueueOptions | boolean | undefined | null>(queues)

    for (const [qName, qOptions] of qIterator) {
      if (qOptions) {
        const queue = new Queue(
          qName,
          {
            ...queueOptions,
            ...(qOptions === true ? undefined : qOptions)
          },
          Connection
        )

        this.queues[qName as QNs] = queue
      }
    }
  }

  on<U extends keyof QueueListener<any, any, string>>(event: U, listener: QueueListener<any, any, string>[U]) {

    for (const qName of Object.keys(this.queues) as QNs[]) {
      const queue = this.getQueue(qName)
      queue.on(event, listener)
    }
  }

  once<U extends keyof QueueListener<any, any, string>>(event: U, listener: QueueListener<any, any, string>[U]) {

    for (const qName of Object.keys(this.queues) as QNs[]) {
      const queue = this.getQueue(qName)
      queue.once(event, listener)
    }
  }

  async waitUntilReady() {
    if (this.connectionStatus != 'connected') {
      this.connectionStatus = 'connected'
    } else {
      throw new Error(`${this.constructor.name} is already connected`)
    }
    await Promise.all(
      Object.values<Queue>(this.queues).map((q) => q.waitUntilReady())
    )
  }

  async close() {
    this.checkConnectionStatus()
    this.connectionStatus = 'closed'

    await Promise.all(
      Object.values<Queue>(this.queues).map((q) => { q.close() }))

  }

  getQueue(name: QNs) {
    return this.queues[name]
  }

  addJob(job: J) {
    this.checkConnectionStatus()

    const queueName = this.getQueueNameByJobName(job.name)
    return this.queues[queueName].add(job.name, job.data, job.opts)

  }

  addJobs(jobs: J[]) {
    this.checkConnectionStatus()

    const jobsPerQueue = {} as Record<QNs, J[] | undefined>
    for (const j of jobs) {
      const queueName = this.getQueueNameByJobName(j.name)
      let jobs = jobsPerQueue[queueName]
      if (!jobs) {
        jobsPerQueue[queueName] = jobs = []
      }
      jobs.push(j)
    }
    return Promise.all(
      Object.entries<J[] | undefined>(jobsPerQueue)
        .map(([queueName, jobs]) => this.queues[queueName as QNs].addBulk(jobs as J[]))
    )
  }

  getQueueNameByJobName(name: JNs) {
    return this.nameToQueue[name]
  }

  private checkConnectionStatus() {
    if (this.connectionStatus === 'disconnected') {
      throw new Error(`${this.constructor.name} is disconnected`)
    } else if (this.connectionStatus === 'closed') {
      throw new Error(`${this.constructor.name} is closed`)
    }
  }

}
