import { describe, it, before, after, afterEach } from 'node:test'
import { equal, deepStrictEqual, rejects, throws } from 'node:assert'

import type { QueueOptions } from 'bullmq'

import { QueueManager } from '../../src/QueueManager'
import type { DefaultJob, NameToQueue, Queues } from '../../src/QueueManager'
import { createRedis } from '../utils'

type JobNames = 'Job1' | 'Job2' | 'Disabled'
type QueueNames = 'Queue1' | 'Queue2' | 'Off'

describe('Queue manager lifecycle', () => {
  const connection = createRedis()

  const queues: Queues<QueueNames> = {
    Queue1: true,
    Queue2: { defaultJobOptions: { attempts: 3 } },
    Off: false,
  }
  const queueOptions: QueueOptions = { connection, prefix: 'bmqt-lifecycle' }
  const nameToQueue: NameToQueue<JobNames, QueueNames> = {
    Job1: 'Queue1',
    Job2: 'Queue2',
    Disabled: 'Off',
  }

  const create = (options?: ConstructorParameters<typeof QueueManager>[3]) =>
    new QueueManager<JobNames, QueueNames, DefaultJob<JobNames>>(queues, queueOptions, nameToQueue, options as any)

  const managers: Array<QueueManager<JobNames, QueueNames, DefaultJob<JobNames>>> = []
  const make = (options?: Parameters<typeof create>[0]) => {
    const m = create(options)
    managers.push(m)
    return m
  }

  before(async () => {
    await connection.connect()
  })

  afterEach(async () => {
    await Promise.all(managers.splice(0).map(m => m.close()))
    for (const name of ['Queue1', 'Queue2']) {
      const m = create()
      await m.waitUntilReady()
      await m.getQueue(name as QueueNames).obliterate({ force: true })
      await m.close()
    }
  })

  after(async () => {
    await connection.quit()
  })

  it('skips queues with falsy config', () => {
    const m = make()
    deepStrictEqual(m.getQueues().map(q => q.name).sort(), ['Queue1', 'Queue2'])
    equal(m.getQueue('Off'), undefined)
  })

  it('throws when used before waitUntilReady()', async () => {
    const m = make()
    throws(() => m.addJob({ name: 'Job1', data: {} }), /is not connected/)
    await rejects(m.addJobs([{ name: 'Job1', data: {} }]), /is not connected/)
  })

  it('waitUntilReady() / close() return whether the state changed', async () => {
    const m = make()
    equal(await m.waitUntilReady(), true)
    equal(await m.waitUntilReady(), false)
    equal(await m.close(), true)
    equal(await m.close(), false)
  })

  it('close() without waitUntilReady() returns false', async () => {
    const m = make()
    equal(await m.close(), false)
  })

  it('throws a clear error for a job routed to a disabled queue', async () => {
    const m = make()
    await m.waitUntilReady()
    throws(() => m.addJob({ name: 'Disabled', data: {} }), /Queue "Off" is not enabled/)
    await rejects(m.addJobs([{ name: 'Disabled', data: {} }]), /Queue "Off" is not enabled/)
  })

  it('applies prepareJob in addJob() and addJobs()', async () => {
    const seen: string[] = []
    const m = make({
      prepareJob: (job) => {
        seen.push(job.name)
        return { ...job, opts: { ...job.opts, jobId: `id-${job.name}-${seen.length}` } }
      },
    })
    await m.waitUntilReady()

    const single = await m.addJob({ name: 'Job1', data: 1 })
    equal(single.id, 'id-Job1-1')

    const bulk = await m.addJobs([{ name: 'Job1', data: 2 }, { name: 'Job2', data: 3 }])
    deepStrictEqual(bulk.map(j => j.id).sort(), ['id-Job1-2', 'id-Job2-3'])
    deepStrictEqual(seen, ['Job1', 'Job1', 'Job2'])
  })

  it('addJobs([]) is a no-op', async () => {
    const m = make()
    await m.waitUntilReady()
    deepStrictEqual(await m.addJobs([]), [])
  })
})
