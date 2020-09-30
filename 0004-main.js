const cluster = require('cluster');
const Events = require('events');
const eventsEmitter = new Events();
const http = require('http');
const QueuesSet = require('./src/DataStructures/QueuesSet');

const RABBIT_HOST = process.env.RABBIT_HOST || '127.0.0.1';
const RABBIT_API_PORT = process.env.RABBIT_API_PORT || '15672';
const RABBIT_PORT = process.env.RABBIT_PORT || '5672';

if (!cluster.isWorker) {
    const retrieveQueues = () => {
        http.get(`http://${RABBIT_HOST}:${RABBIT_API_PORT}/api/queues`, {auth: 'guest:guest'}, (response) => {
            let content = '';
            response.on('data', (chunk) => { content += chunk });
            response.on('end', () => {
                const queues = JSON.parse(content).map((item) => item.name);
                eventsEmitter.emit('http.queues.retrieved', queues);
            });
        });
    };

    const manageProcess = (queue) => {
        const worker = cluster.fork({'QUEUE_NAME': queue});
        const id = worker.id;

        worker.on('online', () => { console.log(`worker ${id} available to queue "${queue}"`) });

        worker.on('exit', (code, signal) => {
            if (code === 0) {
                console.log(`worker ${id} stopped to queue "${queue}"`);
                return;
            }
            console.log(`worker ${id} die to queue "${queue}"`);
            manageProcess(queue);
        });
    };

    const queuesSet = new QueuesSet(eventsEmitter);

    eventsEmitter.on('http.queues.retrieved', (queues) => queuesSet.update(queues));

    eventsEmitter.on('queue.updated', (queues) => retrieveQueues());

    eventsEmitter.on('queue.item.added', (queue) => manageProcess(queue));

    eventsEmitter.on('queue.item.deleted', (queue) => {
        for (const id in cluster.workers) {
            cluster.workers[id].send({'queue': queue, 'command': 'QUIT'})
        }
    });

    /**
     * start application
     */
    console.log('application started');
    retrieveQueues();
} else {
    const rabbitMQConnection = require('amqplib').connect(`amqp://${RABBIT_HOST}:${RABBIT_PORT}`);

    rabbitMQConnection
        .then((connection) => connection.createChannel())
        .then((channel) => {
            launchConsumer(channel, process.env.QUEUE_NAME, (message) => { });
        })
        .catch(() => process.exit(255));

    const launchConsumer = (channel, queue, handler) => {
        channel.consume(queue, handler).catch(() => process.exit(255));
    };

    /**
     * controll worker execution
     */
    process.on('message', (data) => {
        if (data.queue === process.env.QUEUE_NAME && data.command === 'QUIT') {
            process.exit(0);
        }
    });
}