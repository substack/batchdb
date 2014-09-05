var batchdb = require('../');
var db = require('level')('/tmp/compute.db');
var compute = batchdb(db, { path: '/tmp/compute.blobs' });

compute.list('pending').on('data', console.log);
