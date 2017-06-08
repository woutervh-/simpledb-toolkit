import AWS from 'aws-sdk';
import {SimpleDbMutex} from '../src/index';

AWS.config.update({region: 'eu-west-1'});
AWS.config.update({accessKeyId: 'AKIAJQUKFU4OSTBXXFWQ', secretAccessKey: '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', region: 'eu-west-1'});

const mutex = new SimpleDbMutex('my-mutex-test', {domain: 'locks'});

mutex.lock().then((guard1) => {
    console.log('Locked 1.');
    mutex.lock().then((guard2) => {
        console.log('Locked 2.');
        mutex.unlock(guard2).then(() => {
            console.log('Unlocked 2.')
        });
    });
    mutex.unlock(guard1).then(() => {
        console.log('Unlocked 1.');
    });
});
