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
        const RABBIT_EVENTS_EXCHANGE = 'amq.rabbitmq.event';
        const RABBIT_EVENTS_QUEUE = 'rabbitmq.events';
        const RABBIT_EVENTS_ROUTING_KEY = 'queue.*';
        const RABBIT_START_EXCHANGE = 'application.event';
        const RABBIT_START_EXCHANGE_TYPE = 'topic';

        const connectDataSource = (handler) => {
            rabbitMQConnection
                .then((connection) => connection.createChannel())
                .then((channel) => handler(channel));
        };

        const configureQueues = async (channel, queues) => {
            console.log('configuring base queues');

            await channel.assertQueue(RABBIT_EVENTS_QUEUE);
            await channel.bindQueue(RABBIT_EVENTS_QUEUE, RABBIT_EVENTS_EXCHANGE, RABBIT_EVENTS_ROUTING_KEY);
            await channel.assertExchange(RABBIT_START_EXCHANGE, RABBIT_START_EXCHANGE_TYPE);
            await channel.bindQueue(RABBIT_EVENTS_QUEUE, RABBIT_START_EXCHANGE, RABBIT_EVENTS_ROUTING_KEY);

            queues.forEach((queue) => {
                const options = {
                    'headers': {
                        'name': queue
                    }
                };
                channel.publish(RABBIT_START_EXCHANGE, 'queue.created', Buffer.from(''), options);
            });

            await channel.unbindQueue(RABBIT_EVENTS_QUEUE, RABBIT_START_EXCHANGE, RABBIT_EVENTS_ROUTING_KEY);
            await channel.deleteExchange(RABBIT_START_EXCHANGE);

            channel.close();

            eventsEmitter.emit('queues.configured');
        }

        const fetchDataApplication = (channel) => {
            http.get(`http://${RABBIT_HOST}:${RABBIT_API_PORT}/api/queues`, {auth: 'guest:guest'}, (response) => {
                let content = '';
                response.on('data', (chunk) => { content += chunk });
                response.on('end', () => {
                    let queues = JSON.parse(content).map((item) => item.name);
                    queues = queues.filter((queue) => queue !== RABBIT_EVENTS_QUEUE);
                    eventsEmitter.emit('api.data.fetched', channel, queues);
                });
            });
        };

        console.log('application started');

        connectDataSource((channel) => {
            fetchDataApplication(channel);
        });

        /**
         * react to api
         */
        eventsEmitter.on('api.data.fetched', configureQueues);

        eventsEmitter.on('queues.configured', () => {
            connectDataSource((channel) => {
                launchConsumer(channel, RABBIT_EVENTS_QUEUE, (message) => {
                    channel.ack(message);
                    if (message.properties.headers.name !== RABBIT_EVENTS_QUEUE) {
                        eventsEmitter.emit(message.fields.routingKey, message.properties.headers.name);
                    }
                });
            });
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