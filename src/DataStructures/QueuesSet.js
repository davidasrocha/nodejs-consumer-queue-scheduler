class QueuesSet extends Set
{
    #emitter;

    constructor(eventsEmitter)
    {
        super();
        this.#emitter = eventsEmitter;
    }

    add(value)
    {
        const contains = super.has(value);
        super.add(value);

        if (!contains) {
            this.#emitter.emit('queue.item.added', value);
        }
    }

    delete(value)
    {
        if (super.delete(value)) {
            this.#emitter.emit('queue.item.deleted', value);
        }
    }

    update(queues)
    {
        this._prune(new Set(queues));
        queues.forEach((queue) => this.add(queue));
        this.#emitter.emit('queue.updated', this);
    }

    _prune(queuesSet)
    {
        this.forEach((queue) => {
            if (!queuesSet.has(queue)) {
                this.delete(queue);
            }
        });
    }
}

module.exports = QueuesSet;