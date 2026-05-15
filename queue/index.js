const { Queue, Worker } = require('bullmq');
const IORedis = require('ioredis');
let log4js = require('log4js');

let connection = null;
let queues = {};
let workers = [];

const logLevel = process.env.LOG_LEVEL ? process.env.LOG_LEVEL : 'info';
log4js.configure({
    appenders: { out: { type: 'stdout', layout: { type: 'basic' } } },
    categories: { default: { appenders: ['out'], level: logLevel.toUpperCase() } }
});

let version = require('../package.json').version;
let loggerName = process.env.HOSTNAME ? `[${process.env.DATA_STACK_NAMESPACE}] [${process.env.HOSTNAME}] [data.stack-bullmq ${version}]` : `[data.stack-bullmq ${version}]`;
let logger = log4js.getLogger(loggerName);

let init = (config) => {
    const redisConfig = {
        host: config.host || config.redisHost || '127.0.0.1',
        port: config.port || config.redisPort || 6379,
        connectTimeout: config.connectTimeout || 15000,
        maxRetriesPerRequest: config.maxRetriesPerRequest !== undefined ? config.maxRetriesPerRequest : null,
        enableReadyCheck: config.enableReadyCheck !== undefined ? config.enableReadyCheck : true,
    };
    if (config.password) redisConfig.password = config.password;
    if (config.username) redisConfig.username = config.username;
    if (config.tls) redisConfig.tls = config.tls;

    logger.debug(`Redis init config (BullMQ): ${JSON.stringify({ ...redisConfig, password: redisConfig.password ? '[REDACTED]' : undefined })}`);

    connection = new IORedis(redisConfig);
    connection.on('error', function (err) {
        logger.error('Redis error (BullMQ)', err)
    });

    connection.on('connect', function () {
        logger.info('Connected to Redis (BullMQ)');
    });

    connection.on('ready', () => {
        logger.info('Redis is ready for BullMQ operations');
    });

    connection.on('close', function () {
        logger.info('Connection closed to Redis server (BullMQ)');
    });

    connection.on('reconnecting', function () {
        logger.info('Reconnecting to Redis server (BullMQ)');
    });

    return connection;
}

let getQueue = (queueName, config = {}) => {
    const { attempts, ...queueOptions } = config;

    if (!connection) {
        throw new Error('BullMQ Redis connection is not initialized. Call init() first.');
    }
    if (!queues[queueName]) {
        queues[queueName] = new Queue(queueName, {
            connection, defaultJobOptions: {
                removeOnComplete: true,
                removeOnFail: 30,
                attempts: attempts || parseInt(process.env.BULLMQ_JOB_ATTEMPTS) || 3,
                backoff: {
                    type: 'exponential',
                    delay: 2000
                }

            }, ...queueOptions
        });
    }
    return queues[queueName];
}


function registerWorker(queueName, processor, config = {}) {
    const { concurrency, ...workerOptions } = config;
    if (!connection) {
        throw new Error('BullMQ Redis connection is not initialized. Call init() first.');
    }
    const workerConcurrency = parseInt(concurrency) || parseInt(process.env.BULLMQ_WORKER_CONCURRENCY) || 1;

    let workerInstance = new Worker(
        queueName,
        async (job) => {
            try {
                logger.debug(`Processing job`, {
                    queue: queueName,
                    jobId: job.id,
                    name: job.name
                });
                await processor(job.data);
            } catch (err) {
                logger.error(`Error processing job ${job.id}: ${err.message}`);
                throw err;
            }
        },
        { connection, concurrency: workerConcurrency, ...workerOptions }
    );

    workerInstance.on('completed', (job) => logger.debug(`Job ${job.id} completed`, {
        queue: queueName,
        jobId: job.id,
        name: job.name
    }));
    workerInstance.on('failed', (job, err) => logger.error(`Job ${job.id} failed`, err));
    workerInstance.on('error', (err) => {
        logger.error('Worker error -', err);
    });
    workers.push(workerInstance);
    logger.trace(`Worker started (${queueName}) concurrency=${workerConcurrency}`);
    return workerInstance;
}

async function shutdown() {
    logger.info('Shutting down BullMQ...');

    for (const w of workers) {
        await w.close();
    }

    if (connection) {
        await connection.quit();
    }
    logger.info('BullMQ shutdown complete');
}

process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);

module.exports = {
    init: init,
    getQueue: getQueue,
    registerWorker: registerWorker,
    Queue: Queue,
    Worker: Worker
};
