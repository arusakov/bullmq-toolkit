import { FlowProducer, QueueOptions, RedisConnection } from 'bullmq'

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

  protected flowProducer: FlowProducer

  constructor(
    queues: Queues<QNs>,
    queueOptions: QueueOptions,
    protected nameToQueue: NameToQueue<JNs, QNs>,
    options?: Options,
    Connection?: typeof RedisConnection,
  ) {
    super(queues, queueOptions, nameToQueue, options, Connection)

    this.flowProducer = new FlowProducer(queueOptions, Connection)
  }


  async addFlowJob(job: FlowJob<JNs>) {
    this.checkConnectionStatus()
    const flowJobWithQueueNames = this.resolveQueueNames(job)
    return this.flowProducer.add(flowJobWithQueueNames)
  }

  async addFlowJobs(jobs: FlowJob<JNs>[]) {
    this.checkConnectionStatus()
    const flowJobsWithQueueNames = jobs.map(job => this.resolveQueueNames(job))
    return this.flowProducer.addBulk(flowJobsWithQueueNames)
  }

  async waitUntilReady() {
    await Promise.all([
      super.waitUntilReady(),
      this.flowProducer.waitUntilReady(),
    ])
  }

  async close() {
    await Promise.all([
      super.close(),
      this.flowProducer.close(),
    ])
  }

  private resolveQueueNames(job: FlowJob<JNs>): FlowJobReal<JNs> {
    const queueName = this.getQueueNameByJobName(job.name)
    const resolvedJob: FlowJobReal<JNs> = {
      ...job,
      queueName,
      children: job.children?.map(child => this.resolveQueueNames(child))
    }
    return this.prepareJob(resolvedJob)  
  }

}