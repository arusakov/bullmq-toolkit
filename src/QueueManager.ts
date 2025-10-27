import { Queue } from 'bullmq'
import type { QueueOptions, RedisConnection, BaseJobOptions } from 'bullmq'


// TODO use this type everywhere
export type Job<J extends { name: string, data: unknown }> = J & {
  opts?: BaseJobOptions 
}

export type QueueConfig = Partial<QueueOptions> | boolean | undefined | null
export type Queues<QN extends string> = Record<QN, QueueConfig>
export type NameToQueue<JN extends string, QN extends string> = Record<JN, QN>
export type DefaultJob<JN extends string> = {
  name: JN
  data: unknown
  opts?: BaseJobOptions
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
    queuesConfig: Queues<QNs>,
    queueOptions: QueueOptions,
    protected nameToQueue: NameToQueue<JNs, QNs>,
    protected options: Options = {},
    Connection?: typeof RedisConnection,
  ) {
    const configIterator = Object.entries<QueueConfig>(queuesConfig)

    for (const [name, queueConfig] of configIterator) {
      if (queueConfig) {
        const queue = new Queue(
          name,
          {
            ...queueOptions,
            ...(queueConfig === true ? undefined : queueConfig)
          },
          Connection
        )

        this.queues[name as QNs] = queue
      }
    }
  }


  async waitUntilReady() {
    if (this.connectionStatus != 'connected') {
      this.connectionStatus = 'connected'
    } else {
      console.log(`${this.constructor.name} is already connected`)
      return
    }
    await Promise.all(
      this.getQueues().map((q) => q.waitUntilReady())
    )
  }

  async close() {
    this.checkConnectionStatus()
    this.connectionStatus = 'closed'

    await Promise.all(
      this.getQueues().map((q) => { q.close() })
    )
  }

  getQueue(name: QNs) {
    return this.queues[name]
  }

  getQueues() {
    return Object.values<Queue>(this.queues)
  }

  addJob(job: J) {
    this.checkConnectionStatus()

    const queueName = this.getQueueNameByJobName(job.name)
    return this.queues[queueName].add(job.name, job.data, job.opts)

  }

  async addJobs(jobs: J[]) {
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

    const added = await Promise.all(
      Object.entries<J[] | undefined>(jobsPerQueue)
        .map(([queueName, jobs]) => this.queues[queueName as QNs].addBulk(jobs as J[]))
    )

    return added.flat()
  }

  getQueueNameByJobName(name: JNs) {
    return this.nameToQueue[name]
  }

  protected checkConnectionStatus() {
    if (this.connectionStatus === 'disconnected') {
      throw new Error(`${this.constructor.name} is disconnected`)
    } else if (this.connectionStatus === 'closed') {
      throw new Error(`${this.constructor.name} is closed`)
    }
  }

}
