import AWS from 'aws-sdk';

const randomDelay = (fn) => new Promise((resolve) => {
    setTimeout(() => resolve(fn()), Math.random() * 50 + 50);
});

export class SimpleDbMutex {
    constructor(id, {domain}) {
        this.domain = domain;
        this.itemName = `simpledb-toolkit-mutex-${id}`;
        this.simpleDbInstance = new AWS.SimpleDB();

        this.ensureMutexExists = this.simpleDbInstance.putAttributes({
            DomainName: domain,
            ItemName: this.itemName,
            Attributes: [
                {Name: 'state', Value: 'unlocked', Replace: false},
                {Name: 'expires', Value: '0', Replace: false},
                {Name: 'counter', Value: '0', Replace: false}
            ],
            Expected: {Exists: false, Name: 'counter'}
        }).promise().catch((error) => {
            if (error.code !== 'ConditionalCheckFailed') {
                throw error;
            }
        });
    }

    lock({maxTries = 10, timeout = 10000} = {}) {
        let tries = 0;

        const retry = () => {
            if (maxTries >= 1 && (tries++ >= maxTries)) {
                throw {error: 'Lock failed', reason: 'Maximum number of tries exceeded'};
            }

            return this.simpleDbInstance.getAttributes({
                DomainName: this.domain,
                ItemName: this.itemName,
                AttributeNames: ['state', 'expires', 'counter']
            }).promise().then((data) => {
                const state = data.Attributes.find(({Name}) => Name === 'state').Value;
                const expires = parseInt(data.Attributes.find(({Name}) => Name === 'expires').Value, 10);
                const counter = parseInt(data.Attributes.find(({Name}) => Name === 'counter').Value, 10);

                if (state === 'unlocked' || expires < Date.now()) {
                    return this.simpleDbInstance.putAttributes({
                        DomainName: this.domain,
                        ItemName: this.itemName,
                        Attributes: [
                            {Name: 'state', Value: 'locked', Replace: true},
                            {Name: 'expires', Value: (Date.now() + timeout).toString(10), Replace: true},
                            {Name: 'counter', Value: (counter + 1).toString(10), Replace: true}
                        ],
                        Expected: {Exists: true, Name: 'counter', Value: counter.toString(10)}
                    }).promise().then(() => counter + 1);
                } else {
                    return Promise.reject();
                }
            }).catch(() => randomDelay(retry));
        };

        return this.ensureMutexExists.then(() => retry());
    }

    unlock(guard, {maxTries = 10} = {}) {
        let tries = 0;

        const retry = () => {
            if (maxTries >= 1 && (tries++ >= maxTries)) {
                throw {error: 'Unlock failed', reason: 'Maximum number of tries exceeded'};
            }

            return this.simpleDbInstance.getAttributes({
                DomainName: this.domain,
                ItemName: this.itemName,
                AttributeNames: ['state', 'counter']
            }).promise().then((data) => {
                const state = data.Attributes.find(({Name}) => Name === 'state').Value;
                const counter = parseInt(data.Attributes.find(({Name}) => Name === 'counter').Value, 10);

                if (state === 'locked' && counter === guard) {
                    return this.simpleDbInstance.putAttributes({
                        DomainName: this.domain,
                        ItemName: this.itemName,
                        Attributes: [
                            {Name: 'state', Value: 'unlocked', Replace: true},
                            {Name: 'counter', Value: (counter + 1).toString(10), Replace: true}
                        ],
                        Expected: {Exists: true, Name: 'counter', Value: guard.toString(10)}
                    }).promise();
                } else if (guard > counter) {
                    return Promise.reject();
                }
            }).catch(() => randomDelay(retry));
        };

        return this.ensureMutexExists.then(() => retry());
    }
}

export class SimpleDbReadersWriterLock {
    constructor(id, timeout, domain) {
        this.id = id;
        this.timeout = timeout;
        this.domain = domain;

        const createReadLock = simpleDb.putAttributes({
            DomainName: domain,
            ItemName: `simpledb-toolkit-read-lock-${id}`,
            Attributes: [{Name: 'value', Value: '0', Replace: false}],
            Expected: {Exists: false, Name: 'value'}
        }).promise();

        const createGlobalLock = simpleDb.putAttributes();
    }

    beginRead() {

    }
}
