var db = require('level')('/tmp/compute.db');

var compute = require('../')(db, { path: '/tmp/compute.blobs' });
compute.add().end('sleep 5; date');

compute.on('create', function (key) {
    console.log('created', key);
});
