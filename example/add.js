var batchdb = require('../');
var db = require('level')('/tmp/compute.db');

var compute = batchdb(db, { path: '/tmp/compute.blobs' });
compute.add().end('sleep 5; date');

compute.on('create', function (key) {
    console.log('created', key);
});

compute.on('push', function (key, created) {
    console.log('pushed', key, created);
});
