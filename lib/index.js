'use strict';

Object.defineProperty(exports, "__esModule", {
    value: true
});
exports.SimpleDbMutex = exports.SimpleDbAtomicCounter = undefined;

var _awsSdk = require('aws-sdk');

var _awsSdk2 = _interopRequireDefault(_awsSdk);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

const exponentialBackoffDelay = (fn, tries, initial = 10) => new Promise(resolve => {
    setTimeout(() => resolve(fn()), (1 + Math.random()) * initial * Math.pow(2, tries));
});

class SimpleDbAtomicCounter {
    constructor(id, { initialValue = 0, domain }) {
        this.domain = domain;
        this.itemName = `simpledb-toolkit-counter-${id}`;
        this.simpleDbInstance = new _awsSdk2.default.SimpleDB();

        this.ensureCounterExists = this.simpleDbInstance.putAttributes({
            DomainName: domain,
            ItemName: this.itemName,
            Attributes: [{ Name: 'counter', Value: initialValue.toString(10), Replace: false }],
            Expected: { Exists: false, Name: 'counter' }
        }).promise().catch(error => {
            if (error.code !== 'ConditionalCheckFailed') {
                throw error;
            }
        });
    }

    add(amount, { maxTries = 10 } = {}) {
        let tries = 0;

        const retry = () => {
            if (tries++ >= maxTries && maxTries >= 1) {
                throw { error: 'Increment failed', reason: 'Maximum number of tries exceeded' };
            }

            return this.simpleDbInstance.getAttributes({
                DomainName: this.domain,
                ItemName: this.itemName,
                AttributeNames: ['counter']
            }).promise().then(data => {
                const counter = parseInt(data.Attributes[0].Value, 10);
                return this.simpleDbInstance.putAttributes({
                    DomainName: this.domain,
                    ItemName: this.itemName,
                    Attributes: [{ Name: 'counter', Value: (counter + amount).toString(10), Replace: true }],
                    Expected: { Exists: true, Name: 'counter', Value: counter.toString(10) }
                }).promise().then(() => ({ oldValue: counter, newValue: counter + amount }));
            }).catch(() => exponentialBackoffDelay(retry, tries));
        };

        return this.ensureCounterExists.then(() => retry());
    }

    get({ maxTries = 10 } = {}) {
        let tries = 0;

        const retry = () => {
            if (tries++ >= maxTries && maxTries >= 1) {
                throw { error: 'Increment failed', reason: 'Maximum number of tries exceeded' };
            }

            return this.simpleDbInstance.getAttributes({
                DomainName: this.domain,
                ItemName: this.itemName,
                AttributeNames: ['counter']
            }).promise().then(data => {
                return parseInt(data.Attributes[0].Value, 10);
            }).catch(() => exponentialBackoffDelay(retry, tries));
        };

        return this.ensureCounterExists.then(() => retry());
    }

    increment({ maxTries = 10 } = {}) {
        return this.add(1, { maxTries });
    }

    decrement({ maxTries = 10 } = {}) {
        return this.add(-1, { maxTries });
    }
}

exports.SimpleDbAtomicCounter = SimpleDbAtomicCounter;
class SimpleDbMutex {
    constructor(id, { domain }) {
        this.domain = domain;
        this.itemName = `simpledb-toolkit-mutex-${id}`;
        this.simpleDbInstance = new _awsSdk2.default.SimpleDB();

        this.ensureMutexExists = this.simpleDbInstance.putAttributes({
            DomainName: domain,
            ItemName: this.itemName,
            Attributes: [{ Name: 'state', Value: 'unlocked', Replace: false }, { Name: 'expires', Value: '0', Replace: false }, { Name: 'counter', Value: '0', Replace: false }],
            Expected: { Exists: false, Name: 'counter' }
        }).promise().catch(error => {
            if (error.code !== 'ConditionalCheckFailed') {
                throw error;
            }
        });
    }

    lock({ maxTries = 10, ttl = 10000 } = {}) {
        let tries = 0;

        const retry = () => {
            if (tries++ >= maxTries && maxTries >= 1) {
                throw { error: 'Lock failed', reason: 'Maximum number of tries exceeded' };
            }

            return this.simpleDbInstance.getAttributes({
                DomainName: this.domain,
                ItemName: this.itemName,
                AttributeNames: ['state', 'expires', 'counter']
            }).promise().then(data => {
                const state = data.Attributes.find(({ Name }) => Name === 'state').Value;
                const expires = parseInt(data.Attributes.find(({ Name }) => Name === 'expires').Value, 10);
                const counter = parseInt(data.Attributes.find(({ Name }) => Name === 'counter').Value, 10);

                if (state === 'unlocked' || expires < Date.now()) {
                    return this.simpleDbInstance.putAttributes({
                        DomainName: this.domain,
                        ItemName: this.itemName,
                        Attributes: [{ Name: 'state', Value: 'locked', Replace: true }, { Name: 'expires', Value: (Date.now() + ttl).toString(10), Replace: true }, { Name: 'counter', Value: (counter + 1).toString(10), Replace: true }],
                        Expected: { Exists: true, Name: 'counter', Value: counter.toString(10) }
                    }).promise().then(() => counter + 1);
                } else {
                    return Promise.reject();
                }
            }).catch(() => exponentialBackoffDelay(retry, tries));
        };

        return this.ensureMutexExists.then(() => retry());
    }

    unlock(guard, { maxTries = 10 } = {}) {
        let tries = 0;

        const retry = () => {
            if (tries++ >= maxTries && maxTries >= 1) {
                throw { error: 'Unlock failed', reason: 'Maximum number of tries exceeded' };
            }

            return this.simpleDbInstance.getAttributes({
                DomainName: this.domain,
                ItemName: this.itemName,
                AttributeNames: ['state', 'counter']
            }).promise().then(data => {
                const state = data.Attributes.find(({ Name }) => Name === 'state').Value;
                const counter = parseInt(data.Attributes.find(({ Name }) => Name === 'counter').Value, 10);

                if (state === 'locked' && counter === guard) {
                    return this.simpleDbInstance.putAttributes({
                        DomainName: this.domain,
                        ItemName: this.itemName,
                        Attributes: [{ Name: 'state', Value: 'unlocked', Replace: true }, { Name: 'counter', Value: (counter + 1).toString(10), Replace: true }],
                        Expected: { Exists: true, Name: 'counter', Value: guard.toString(10) }
                    }).promise();
                } else if (guard > counter) {
                    return Promise.reject();
                }
            }).catch(() => exponentialBackoffDelay(retry, tries));
        };

        return this.ensureMutexExists.then(() => retry());
    }
}
exports.SimpleDbMutex = SimpleDbMutex;