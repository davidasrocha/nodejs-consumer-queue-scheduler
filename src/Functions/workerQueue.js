const cluster = require('cluster');

const startWorkerQueue = (queue) => {
    const worker = cluster.fork({'QUEUE_NAME': queue});
    const id = worker.id;

    worker.on('online', () => { console.log(`worker ${id} available to queue "${queue}"`) });

    worker.on('exit', (code, signal) => {
        if (code === 0) {
            console.log(`worker ${id} stopped to queue "${queue}"`);
            return;
        }
        console.log(`worker ${id} die to queue "${queue}"`);
        startWorkerQueue(queue);
    });
};

const stopWorkerQueue = (queue) => {
    for (const id in cluster.workers) {
        cluster.workers[id].send({'queue': queue, 'command': 'QUIT'})
    }
};

module.exports = { startWorkerQueue, stopWorkerQueue };