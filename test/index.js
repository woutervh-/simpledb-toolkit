import AWS from 'aws-sdk';
import {SimpleDbMutex, SimpleDbAtomicCounter} from '../src/index';

AWS.config.update({region: 'eu-west-1'});
AWS.config.update({accessKeyId: '', secretAccessKey: '', region: 'eu-west-1'});

const mutex = new SimpleDbMutex('my-mutex-test', {domain: 'locks'});
const counter = new SimpleDbAtomicCounter('my-counter-text', {domain: 'locks', initialValue: 3});

const wait = (amount) => new Promise((resolve) => {
    setTimeout(() => resolve(), amount);
});

mutex.lock().then((guard1) => {
    console.log('Locked 1.');
    mutex.lock().then((guard2) => {
        console.log('Locked 2.');
        mutex.unlock(guard2).then(() => {
            console.log('Unlocked 2.')
        });
    });
    wait(5000).then(() => mutex.unlock(guard1).then(() => {
        console.log('Unlocked 1.');
    }));
});

counter.decrement().then((result) => {
    console.log(result);
    counter.get().then((result) => console.log(result))
}).catch((error) => console.error(error));


