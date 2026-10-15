import { Queue } from 'bullmq'
import type { QueueOptions, BaseJobOptions, IQueueBackend } from 'bullmq'


// TODO use this type everywhere
export type Job<J extends { name: string, data: unknown }> = J & {
  opts?: BaseJobOptions 
}

/** `Queue` as inferred by bullmq's constructor when no custom backend is given */
export type ManagedQueue = Queue<any, any, string, any, any, string, IQueueBackend>

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


export class QueueManager<
  JNs extends string,
  QNs extends string,
  J extends DefaultJob<JNs>,
> {
  protected queues = {} as Record<QNs, ManagedQueue>
  protected connected = false
  protected nameToQueue: NameToQueue<JNs, QNs>

  constructor(
    queuesConfig: Queues<QNs>,
    queueOptions: QueueOptions,
    nameToQueue: NameToQueue<JNs, QNs>,
  ) {
    this.nameToQueue = nameToQueue
    const configIterator = Object.entries<QueueConfig>(queuesConfig)

    for (const [name, queueConfig] of configIterator) {
      if (queueConfig) {
        const queue = new Queue(
          name,
          {
            ...queueOptions,
            ...(queueConfig === true ? undefined : queueConfig)
          },
        )

        this.queues[name as QNs] = queue
      }
    }
  }


  async waitUntilReady() {
    if (this.connected) {
      return false
    }
    await Promise.all(
      this.getQueues().map((q) => q.waitUntilReady())
    )

    this.connected = true
    return true
  }

  async close() {
    const wasConnected = this.connected

    // always close queues: waitUntilReady() could have failed after opening some connections
    await Promise.all(
      this.getQueues().map((q) => q.close())
    )
    this.connected = false
    return wasConnected
  }

  getQueue(name: QNs) {
    return this.queues[name]
  }

  getQueues() {
    return Object.values<ManagedQueue>(this.queues)
  }

  addJob(job: J) {
    this.checkConnected()

    const queueName = this.getQueueNameByJobName(job.name)
    return this.queues[queueName].add(job.name, job.data, job.opts)

  }

  async addJobs(jobs: J[]) {
    this.checkConnected()

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

  protected checkConnected() {
    if (!this.connected) {
      throw new Error(`${this.constructor.name} is not connected`)
    }
  }
}
