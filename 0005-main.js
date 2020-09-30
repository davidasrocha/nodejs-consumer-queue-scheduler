const cluster = require('cluster');
const Events = require('events');
const eventsEmitter = new Events();
const QueuesSet = require('./src/DataStructures/QueuesSet');
const { startWorkerQueue, stopWorkerQueue } = require('./src/Functions/workerQueue');

const RABBIT_HOST = process.env.RABBIT_HOST || '127.0.0.1';
const RABBIT_PORT = process.env.RABBIT_PORT || '5672';
const RABBIT_EVENTS_EXCHANGE = 'amq.rabbitmq.event';
const RABBIT_EVENTS_QUEUE = 'rabbitmq.events';
const RABBIT_EVENTS_ROUTING_KEY = 'queue.*';

const launchConsumer = (channel, queue, handler) => {
    channel.consume(queue, handler).catch(() => process.exit(255));
};

if (!cluster.isWorker) {
    const rabbitMQConnection = require('amqplib').connect(`amqp://${RABBIT_HOST}:${RABBIT_PORT}`);

    rabbitMQConnection
        .then((connection) => connection.createChannel())
        .then(async (channel) => {
            await channel.assertQueue(RABBIT_EVENTS_QUEUE);
            await channel.bindQueue(RABBIT_EVENTS_QUEUE, RABBIT_EVENTS_EXCHANGE, RABBIT_EVENTS_ROUTING_KEY);
            
            launchConsumer(channel, RABBIT_EVENTS_QUEUE, (message) => {
                channel.ack(message);
                if (message.properties.headers.name !== RABBIT_EVENTS_QUEUE) {
                    eventsEmitter.emit(message.fields.routingKey, message.properties.headers.name);
                }
            });
        })
        .catch(() => process.exit(255));

    const queuesSet = new QueuesSet(eventsEmitter);

    /**
     * react to rabbitmq events
     */
    eventsEmitter.on('queue.created', (queue) => queuesSet.add(queue));
    eventsEmitter.on('queue.deleted', (queue) => queuesSet.delete(queue));

    /**
     *  control worker process
     */
    eventsEmitter.on('queue.item.added', startWorkerQueue);
    eventsEmitter.on('queue.item.deleted', stopWorkerQueue);

    /**
     * start application
     */
    console.log('application started');
} else {
    const rabbitMQConnection = require('amqplib').connect(`amqp://${RABBIT_HOST}:${RABBIT_PORT}`);

    rabbitMQConnection
        .then((connection) => connection.createChannel())
        .then((channel) => {
            launchConsumer(channel, process.env.QUEUE_NAME, (message) => { });
        })
        .catch(() => process.exit(255));

    /**
     * controll worker execution
     */
    process.on('message', (data) => {
        if (data.queue === process.env.QUEUE_NAME && data.command === 'QUIT') {
            process.exit(0);
        }
    });
}