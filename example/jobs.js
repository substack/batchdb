var db = require('level')('/tmp/compute.db');
var compute = require('../')(db, { path: '/tmp/compute.blobs' });

compute.jobs().on('data', console.log);
