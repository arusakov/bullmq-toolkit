import { Redis } from 'ioredis'

export const createRedis = () => {
  return new Redis({
    lazyConnect: true,
    maxRetriesPerRequest: null,
    host: process.env.REDIS_HOST,
    port: Number(process.env.REDIS_PORT) || undefined,
    disconnectTimeout: 0,
  })
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))