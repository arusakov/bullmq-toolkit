import { describe, it, before, after, afterEach } from 'node:test'
import { equal, deepStrictEqual, rejects } from 'node:assert'

import type { QueueOptions } from 'bullmq'

import type { DefaultJob, FlowJob, NameToQueue, Queues } from '../../src/QueueManager'
import { QueueFlowManager } from '../../src/QueueFlowManager'
import { createRedis } from '../utils'

type JobNames = 'Job1' | 'Job2' | 'Disabled'
type QueueNames = 'Queue1' | 'Queue2' | 'Off'

describe('Queue flow manager lifecycle', () => {
  const connection = createRedis()

  const queues: Queues<QueueNames> = { Queue1: true, Queue2: true, Off: false }
  const queueOptions: QueueOptions = { connection, prefix: 'bmqt-flow-lifecycle' }
  const nameToQueue: NameToQueue<JobNames, QueueNames> = {
    Job1: 'Queue1',
    Job2: 'Queue2',
    Disabled: 'Off',
  }

  const managers: Array<QueueFlowManager<JobNames, QueueNames, DefaultJob<JobNames>>> = []
  const make = (options?: ConstructorParameters<typeof QueueFlowManager>[3]) => {
    const m = new QueueFlowManager<JobNames, QueueNames, DefaultJob<JobNames>>(
      queues, queueOptions, nameToQueue, options as any,
    )
    managers.push(m)
    return m
  }

  before(async () => {
    await connection.connect()
  })

  afterEach(async () => {
    await Promise.all(managers.splice(0).map(m => m.close()))
    await connection.flushdb()
  })

  after(async () => {
    await connection.quit()
  })

  it('throws when used before waitUntilReady()', async () => {
    const m = make()
    await rejects(m.addFlowJob({ name: 'Job1', data: {} }), /is not connected/)
    await rejects(m.addFlowJobs([{ name: 'Job1', data: {} }]), /is not connected/)
  })

  it('waitUntilReady() / close() return whether the state changed', async () => {
    const m = make()
    equal(await m.close(), false)
    equal(await m.waitUntilReady(), true)
    equal(await m.waitUntilReady(), false)
    equal(await m.close(), true)
    equal(await m.close(), false)
  })

  it('adds a flow job without children', async () => {
    const m = make()
    await m.waitUntilReady()
    const node = await m.addFlowJob({ name: 'Job1', data: {} })
    equal(node.job.queueName, 'Queue1')
    equal(node.children, undefined)
  })

  it('applies prepareJob to every node of the flow', async () => {
    const seen: string[] = []
    const m = make({
      prepareJob: (job) => {
        seen.push(job.name)
        return job
      },
    })
    await m.waitUntilReady()

    const flow: FlowJob<JobNames> = {
      name: 'Job1',
      data: {},
      children: [{ name: 'Job2', data: {} }, { name: 'Job2', data: {} }],
    }
    const node = await m.addFlowJob(flow)

    equal(node.children?.length, 2)
    deepStrictEqual(node.children?.map(c => c.job.queueName), ['Queue2', 'Queue2'])
    deepStrictEqual([...seen].sort(), ['Job1', 'Job2', 'Job2'])
  })

  it('throws a clear error for a disabled queue', async () => {
    const m = make()
    await m.waitUntilReady()
    await rejects(
      m.addFlowJob({ name: 'Job1', data: {}, children: [{ name: 'Disabled', data: {} }] }),
      /Queue "Off" is not enabled/,
    )
  })
})
