// 
// this program is responsible to schedule process
// start 01 process to manage queues states
// start N process to manage queue consumer
// 

const cluster = require('cluster');
const Events = require('events');
const eventsEmitter = new Events();
const http = require('http');
const QueuesSet = require('./src/DataStructures/QueuesSet');
const { startWorkerQueue, stopWorkerQueue } = require('./src/Functions/workerQueue');

const RABBIT_HOST = process.env.RABBIT_HOST || '127.0.0.1';
const RABBIT_API_PORT = process.env.RABBIT_API_PORT || '15672';
const RABBIT_PORT = process.env.RABBIT_PORT || '5672';

const launchConsumer = (channel, queue, handler) => {
    channel.consume(queue, handler).catch(() => process.exit(255));
};

if (!cluster.isWorker) {
    const rabbitMQConnection = require('amqplib').connect(`amqp://${RABBIT_HOST}:${RABBIT_PORT}`);

    const startApplication = () => {
        const EXCLUSIVE_QUEUE_PREFIX_NAME = 'amq.gen';
        
        const configureQueues = async (channel, queues) => {
            const RABBIT_EVENTS_EXCHANGE = 'amq.rabbitmq.event';
            const RABBIT_EVENTS_ROUTING_KEY = 'queue.*';
    
            const resultQueue = await channel.assertQueue('', { exclusive: true });
            const queueName = resultQueue.queue;
            const exchangeName = `app.${queueName}.event`;

            await channel.assertExchange(exchangeName, 'topic', { autoDelete: true });
            await channel.bindQueue(queueName, RABBIT_EVENTS_EXCHANGE, RABBIT_EVENTS_ROUTING_KEY);
            await channel.bindQueue(queueName, exchangeName, RABBIT_EVENTS_ROUTING_KEY);

            queues.forEach((queue) => {
                const options = {
                    'headers': {
                        'name': queue
                    }
                };
                channel.publish(exchangeName, 'queue.created', Buffer.from(''), options);
            });

            await channel.unbindQueue(queueName, exchangeName, RABBIT_EVENTS_ROUTING_KEY);

            eventsEmitter.emit('queues.configured', queueName);
        }

        const fetchDataApplication = () => {
            http.get(`http://${RABBIT_HOST}:${RABBIT_API_PORT}/api/queues`, {auth: 'guest:guest'}, (response) => {
                let content = '';
                response.on('data', (chunk) => { content += chunk });
                response.on('end', () => {
                    let queues = JSON.parse(content).map((item) => item.name);
                    queues = queues.filter((queue) => queue.indexOf(EXCLUSIVE_QUEUE_PREFIX_NAME) === -1);
                    eventsEmitter.emit('api.data.fetched', queues);
                });
            });
        };

        const consumerQueueEventsHandler = (channel, queue) => {
            channel.consume(queue, (message) => {
                channel.ack(message);

                if (message.properties.headers.name.indexOf(EXCLUSIVE_QUEUE_PREFIX_NAME) !== -1 
                    || message.properties.headers.name === queue) {
                    return;
                }

                eventsEmitter.emit(message.fields.routingKey, message.properties.headers.name);
            });
        };

        console.log('application started');

        rabbitMQConnection
            .then((connection) => connection.createChannel())
            .then((channel) => {
                fetchDataApplication();

                eventsEmitter.on('api.data.fetched', (queues) => configureQueues(channel, queues));
                eventsEmitter.on('queues.configured', (queue) => consumerQueueEventsHandler(channel, queue));
            });
    };

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
    startApplication();
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