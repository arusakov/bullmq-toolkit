import { FlowProducer } from 'bullmq'
import type { QueueOptions, IQueueBackend } from 'bullmq'

import type { DefaultJob, NameToQueue, Options, Queues } from './QueueManager'

import { QueueManager, FlowJob } from './QueueManager'

export type FlowJobReal<JN extends string> = FlowJob<JN> & {
  queueName: string
  children?: Array<FlowJobReal<JN>>
}

export class QueueFlowManager<
  JNs extends string,
  QNs extends string,
  J extends DefaultJob<JNs>,
> extends QueueManager<JNs, QNs, J> {

  protected flowProducer: FlowProducer<IQueueBackend>

  constructor(
    queues: Queues<QNs>,
    queueOptions: QueueOptions,
    nameToQueue: NameToQueue<JNs, QNs>,
    options?: Options,
  ) {
    super(queues, queueOptions, nameToQueue, options)

    this.flowProducer = new FlowProducer(queueOptions)
  }


  async addFlowJob(job: FlowJob<JNs>) {
    this.checkConnected()

    const flowJobWithQueueNames = this.resolveQueueNames(job)
    return this.flowProducer.add(flowJobWithQueueNames)
  }

  async addFlowJobs(jobs: FlowJob<JNs>[]) {
    this.checkConnected()

    const flowJobsWithQueueNames = jobs.map(job => this.resolveQueueNames(job))
    return this.flowProducer.addBulk(flowJobsWithQueueNames)
  }

  async waitUntilReady() {
    if (this.connected) {
      return false
    }
    await Promise.all([
      super.waitUntilReady(),
      this.flowProducer.waitUntilReady(),
    ])
    return true
  }

  async close() {
    const [wasConnected] = await Promise.all([
      super.close(),
      this.flowProducer.close(),
    ])
    return wasConnected
  }

  private resolveQueueNames(job: FlowJob<JNs>): FlowJobReal<JNs> {
    const queueName = this.getQueueNameByJobName(job.name)
    this.getEnabledQueue(queueName)

    const { children, ...rest } = job
    const resolvedJob: FlowJobReal<JNs> = { ...rest, queueName }
    if (children) {
      resolvedJob.children = children.map(child => this.resolveQueueNames(child))
    }
    return this.prepareJob(resolvedJob)  
  }

}