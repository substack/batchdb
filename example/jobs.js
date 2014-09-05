var db = require('level')('/tmp/compute.db');
var compute = require('../')(db, { path: '/tmp/compute.blobs' });

compute.list('job').on('data', console.log);
