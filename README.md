# 🛠️ bullmq-toolkit

[![test](https://github.com/arusakov/bullmq-toolkit/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/arusakov/bullmq-toolkit/actions?query=workflow%3ATest)
[![codecov](https://codecov.io/gh/arusakov/bullmq-toolkit/graph/badge.svg?token=9GSSXOT9K5)](https://codecov.io/gh/arusakov/bullmq-toolkit)

Small, typed helper classes for managing **many [BullMQ](https://docs.bullmq.io) queues and workers as one unit**.

- Describe all queues once, share common options, override per queue.
- Add jobs by **job name** — the toolkit routes each job to the right queue.
- Add flows (parent/children jobs) with the same name-based routing.
- Start / stop all workers together.
- 100% test coverage, zero runtime dependencies besides the `bullmq` peer.

## Installation

```bash
npm install bullmq-toolkit bullmq ioredis
```

Requires `bullmq` `^6.0.0` (peer dependency). The Redis driver is **not** installed by this package:
use `ioredis`, `node-redis` or Bun's built-in client and pass it via the `connection` option.
BullMQ detects the driver automatically.

> Upgrading from 0.x? See [Migration](#migration-from-0x).

## Quick start

```ts
import { QueueManager, WorkerManager } from 'bullmq-toolkit'
import type { DefaultJob, NameToQueue, Queues, Workers } from 'bullmq-toolkit'
import { Redis } from 'ioredis'

type JobNames = 'SendEmail' | 'ResizeImage'
type QueueNames = 'emails' | 'images'

const connection = new Redis({ maxRetriesPerRequest: null })

// 1. which queues exist (true = use common options, object = override, false = disabled)
const queues: Queues<QueueNames> = {
  emails: true,
  images: { defaultJobOptions: { attempts: 5 } },
}

// 2. which job goes to which queue
const nameToQueue: NameToQueue<JobNames, QueueNames> = {
  SendEmail: 'emails',
  ResizeImage: 'images',
}

const queueManager = new QueueManager<JobNames, QueueNames, DefaultJob<JobNames>>(
  queues,
  { connection },  // common QueueOptions
  nameToQueue,
)
await queueManager.waitUntilReady()

await queueManager.addJob({ name: 'SendEmail', data: { to: 'a@b.c' } })
await queueManager.addJobs([
  { name: 'SendEmail', data: { to: 'x@y.z' } },
  { name: 'ResizeImage', data: { id: 1 } },   // goes to "images"
])

// 3. workers
const workers: Workers<QueueNames> = { emails: true, images: { concurrency: 10 } }

const workerManager = new WorkerManager<JobNames, QueueNames, DefaultJob<JobNames>>(
  workers,
  async (job) => { /* job.name tells what to do */ },
  { connection, concurrency: 1 },  // common WorkerOptions
)
await workerManager.waitUntilReady()
workerManager.run()

// shutdown
await workerManager.close()
await queueManager.close()
```

## API

### Configuration values

`Queues<QN>` / `Workers<QN>` map a queue name to:

| value          | meaning                                                   |
| -------------- | --------------------------------------------------------- |
| `true`         | create it with the common options                         |
| `{ ...opts }`  | create it, shallow-merging `opts` over the common options |
| `false` / `null` / `undefined` | do not create it in this process          |

Disabling a queue is handy when one codebase runs in several processes (e.g. a
web process that only produces jobs vs. a worker process that only consumes some queues).

### `QueueManager<JNs, QNs, J>`

```ts
new QueueManager(queues, queueOptions, nameToQueue)
```

| member                  | description                                                                                      |
| ----------------------- | ------------------------------------------------------------------------------------------------ |
| `waitUntilReady()`      | Waits for all queues. Resolves `true` on the first call, `false` if already ready.               |
| `close()`               | Closes all queues. Resolves `true` if it was ready, `false` otherwise. Safe to call repeatedly.  |
| `addJob(job)`           | Adds one job to the queue resolved from `job.name`. Returns BullMQ `Job`.                        |
| `addJobs(jobs)`         | Groups jobs per queue and uses `addBulk`. Returns a flat array of `Job`.                         |
| `getQueue(name)`        | Returns the underlying BullMQ `Queue` (`undefined` if disabled).                                 |
| `getQueues()`           | Returns all created queues.                                                                      |
| `getQueueNameByJobName` | Resolves a queue name by job name.                                                               |

Adding jobs before `waitUntilReady()` (or after `close()`) throws `... is not connected`.
Do not route jobs to a queue you disabled in this process — list only the queues you enable in `QNs`/`nameToQueue`.

**Defaults for jobs** — use your own job creator functions (or subclass and override `addJob` / `addJobs`):

```ts
const emailJob = (to: string): DefaultJob<'SendEmail'> => ({
  name: 'SendEmail',
  data: { to },
  opts: { removeOnComplete: true },
})

await queueManager.addJob(emailJob('a@b.c'))
```

### `QueueFlowManager<JNs, QNs, J>`

Extends `QueueManager` and additionally owns a BullMQ `FlowProducer`.
Queue names are resolved from job names for the whole tree, so you never write `queueName` by hand.

```ts
const flow = await flowManager.addFlowJob({
  name: 'SendEmail',
  data: {},
  children: [{ name: 'ResizeImage', data: { id: 1 } }],
})

await flowManager.addFlowJobs([flowA, flowB])
```

`waitUntilReady()` / `close()` handle both queues and the flow producer.

### `WorkerManager<JNs, QNs, J>`

```ts
new WorkerManager(workers, processor, workerOptions)
```

| member             | description                                                                                         |
| ------------------ | --------------------------------------------------------------------------------------------------- |
| `waitUntilReady()` | Waits for all workers.                                                                              |
| `run()`            | Starts every worker that is not running yet (use with `autorun: false`). Idempotent.                |
| `close()`          | Gracefully closes all workers. Safe to call repeatedly.                                             |
| `getWorker(name)`  | Returns the BullMQ `Worker` (`undefined` if disabled).                                              |
| `getWorkers()`     | Returns all created workers.                                                                        |

A fatal error of `worker.run()` is emitted as the worker's `error` event, so it never becomes an unhandled rejection.

### Redis client

Pass the client via `connection` in `queueOptions` / `workerOptions` (or per queue in `queues` / `workers`).
BullMQ v6 detects the driver itself: ioredis, node-redis and Bun clients are supported.

## Migration from 0.x

- **ESM-only**: the package is now `"type": "module"`. CJS users on Node 22+ can still `require()` it.
- `bullmq` peer dependency is now `^6.0.0`.
- The last constructor argument `Connection?: typeof RedisConnection` was removed (no equivalent in BullMQ v6);
  pass a client via the `connection` option instead.
- `QueueManager.close()` now always closes the queues (previously it was a no-op if `waitUntilReady()` had not completed, which could leak connections).
- Jobs routed to a disabled queue throw a descriptive error instead of a `TypeError`.
- `Options` / `prepareJob` hook was removed from `QueueManager` and `QueueFlowManager`: build jobs with your own creator functions or override `addJob` / `addJobs`.
- `WorkerManager`'s unused `options` argument and `WorkerManagerOptions` type were removed.

## Development

```bash
docker compose up -d        # Redis on 127.0.0.1:6379
npm run compile
npm run test:all            # REDIS_HOST / REDIS_PORT are respected
npm run test:coverage
```

Tests run on plain `node --test` (native TypeScript type stripping, Node 24+); no ts-node needed.

> Tests call `FLUSHDB` — never point them at a Redis with data you care about.

## License

MIT
