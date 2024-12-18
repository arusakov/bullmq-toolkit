import { Queue, QueueListener, Job } from 'bullmq'
import type { QueueOptions, RedisConnection, DefaultJobOptions } from 'bullmq'


type AddQueueParameter<T extends any[]> = [queue: Queue<any, any, string>, ...T]
type QueueEventName = keyof QueueListener

type ListenerParametersWithQueue<U extends QueueEventName> = AddQueueParameter<Parameters<QueueListener[U]>>

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

const listenerSymbol = Symbol('listenerSymbol')

type QueueFunctionWithSymbol<U extends QueueEventName> = {
  (...args: ListenerParametersWithQueue<U>): void
  [listenerSymbol]?: {
    event: U
    listeners: Function[]
  }
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

  on<U extends QueueEventName>(event: U, listener: QueueFunctionWithSymbol<U>) {

    if (!listener[listenerSymbol]) {
      listener[listenerSymbol] = {
        event: event,
        listeners: [],
      }
    }

    for (const queue of Object.values(this.queues) as Queue[]) {

      const wrappedListener: QueueListener[U] = ((...args: Parameters<QueueListener[U]>) => {
        listener(queue, ...args)
      }) as QueueListener[U]

      listener[listenerSymbol].listeners.push(wrappedListener)
      queue.on(event, wrappedListener)
    }
  }

  off<U extends QueueEventName>(event: U, listener: QueueFunctionWithSymbol<U>) {
    if (!listener[listenerSymbol]) {
      throw new Error('Listener not found')
    }

    const { listeners } = listener[listenerSymbol]

    for (const [index, queue] of (Object.values(this.queues) as Queue[]).entries()) {
      const wrappedListener = listeners[index]
      if (wrappedListener) {
        queue.off(event, wrappedListener as QueueListener[U])
      }
    }
    listener[listenerSymbol].listeners = []
  }


  once<U extends QueueEventName>(event: U, listener: QueueFunctionWithSymbol<U>) {

    for (const queue of Object.values(this.queues) as Queue[]) {
      const wrappedListener: QueueListener[U] = ((...args: Parameters<QueueListener[U]>) => {
        listener(queue, ...args)
      }) as QueueListener[U]
      queue.once(event, wrappedListener)
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

  protected checkConnectionStatus() {
    if (this.connectionStatus === 'disconnected') {
      throw new Error(`${this.constructor.name} is disconnected`)
    } else if (this.connectionStatus === 'closed') {
      throw new Error(`${this.constructor.name} is closed`)
    }
  }

}
