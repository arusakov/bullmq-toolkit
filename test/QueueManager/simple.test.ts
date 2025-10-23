import { describe, it, before, after, afterEach } from 'node:test'
import { equal, throws, rejects } from 'assert'
import { QueueOptions, Job, Queue } from 'bullmq'

import { QueueManager, DefaultJob, Queues, NameToQueue } from '../../src/QueueManager'
import { createRedis } from '../utils'

describe('Queue manager', () => {
  type JobNames = 'Job1' | 'Job2'
  type QueueNames = 'Queue1' | 'Queue2'

  const connection = createRedis()

  let isListenerCalled = false
  let isListenerCalled2 = false
  const listenerOn = (job: Job) => {
    isListenerCalled = true
    console.log(`Job=${job.name} is waiting in queue=${job.queueName}`)
  }

  const listenerOn2 = (job: Job) => {
    isListenerCalled2 = true
    console.log(`№2 Job=${job.name} is waiting in queue=${job.queueName}`)
  }

  let queueManager: QueueManager<JobNames, QueueNames, DefaultJob<JobNames>>
  const newJobForQueue1: DefaultJob<JobNames> = { name: 'Job1', data: {} }
  const newJobForQueue2: DefaultJob<JobNames> = { name: 'Job2', data: {} }

  before(async () => {

    await connection.connect()
    const queues: Queues<QueueNames> = {
      Queue1: {
        connection: connection,
        defaultJobOptions: {
          attempts: 5
        },
      },
      Queue2: true,
    };

    const queueOptions: QueueOptions = {
      connection: connection,
      streams: {
        events: {
          maxLen: 0
        }
      },
      defaultJobOptions: {
        attempts: 0
      }
    }

    const nameToQueue: NameToQueue<JobNames, QueueNames> = {
      Job1: 'Queue1',
      Job2: 'Queue2',
    }

    queueManager = new QueueManager<JobNames, QueueNames, DefaultJob<JobNames>>(
      queues,
      queueOptions,
      nameToQueue,
    )

    await queueManager.waitUntilReady()
  })

  afterEach(async () => {
    await queueManager.getQueue('Queue1').drain()
    await queueManager.getQueue('Queue2').drain()
  })

  after(async () => {

    await connection.quit()
  })

  it('Setup options', () => {
    const queue = queueManager.getQueue('Queue1')
    equal(queue.defaultJobOptions.attempts, 5)

    const queue2 = queueManager.getQueue('Queue2')
    equal(queue2.defaultJobOptions.attempts, 0)
  })

  it('Add job in queue', async () => {

    const job = await queueManager.addJob(newJobForQueue1)
    const queue1Jobs = await queueManager.getQueue('Queue1').getWaiting()

    if (job) {
      equal(job.queueName, 'Queue1')
      equal(job.name, 'Job1')
      equal(queue1Jobs.length, 1)

    }
  })

  it('Add jobs in queue', async () => {
    const newJobs: DefaultJob<JobNames>[] = [{ name: 'Job1', data: {} }, { name: 'Job1', data: {} }, { name: 'Job2', data: {} }]

    await queueManager.addJobs(newJobs)

    const queue1Jobs = await queueManager.getQueue('Queue1').getWaiting()
    const queue2Jobs = await queueManager.getQueue('Queue2').getWaiting()

    equal(queue1Jobs.length, 2)
    equal(queue2Jobs.length, 1)
  })

  it('should return the correct queue name for a given job name', () => {
    const queueName = queueManager.getQueueNameByJobName('Job1')
    equal(queueName, 'Queue1')

    const queueName2 = queueManager.getQueueNameByJobName('Job2')
    equal(queueName2, 'Queue2')
  })

  it('should return the correct queue for a given queue name', () => {
    const queue1 = queueManager.getQueue('Queue1')
    equal(queue1.name, 'Queue1')

    const queue2 = queueManager.getQueue('Queue2')
    equal(queue2.name, 'Queue2')
  })

  it('listener on', async () => {

    queueManager.getQueues().forEach(q => {
      q.on('waiting', listenerOn)
    })

    await queueManager.addJob(newJobForQueue1)
    await queueManager.addJob(newJobForQueue2)
    await new Promise(resolve => setTimeout(resolve, 100))

    const listenersArray1 = queueManager.getQueue('Queue1').listeners('waiting')
    equal(listenersArray1.length, 1)
    equal(isListenerCalled, true)

    const listenersArray2 = queueManager.getQueue('Queue2').listeners('waiting')
    equal(listenersArray2.length, 1)
  })

  it('listener on many 2 cb for event', async () => {

    queueManager.getQueues().forEach(q => {
      q.on('waiting', listenerOn2)
    })

    await queueManager.addJob(newJobForQueue1)
    await new Promise(resolve => setTimeout(resolve, 100))

    const listenersArray = queueManager.getQueue('Queue1').listeners('waiting')
    equal(listenersArray.length, 2)
    equal(isListenerCalled, true)
    equal(isListenerCalled2, true)
  })



  it('listener off', async () => {
    isListenerCalled = false
    isListenerCalled2 = false
    queueManager.getQueues().forEach(q => {
      q.off('waiting', listenerOn)
    })

    await queueManager.addJob(newJobForQueue1)
    await new Promise(resolve => setTimeout(resolve, 100))

    const listenersArray = queueManager.getQueue('Queue1').listeners('waiting')
    equal(listenersArray.length, 1)
    equal(isListenerCalled, false)
    equal(isListenerCalled2, true)
  })


  it('listener once', async () => {
    let callCount = 0

    queueManager.getQueues().forEach(q => {
      q.once('paused', () => {
        callCount++
        console.log(`Queue=${q.name} paused`)
      })
    })

    await queueManager.getQueue('Queue1').pause()
    await queueManager.getQueue('Queue1').pause()

    equal(callCount, 1)
    await queueManager.getQueue('Queue1').resume()

  })

  it('close', async () => {
    await queueManager.close()

    await rejects(async () => await queueManager.addJob(newJobForQueue1), Error)

  })
})
