#!/usr/bin/env node

var path = require('path');
var db = require('level')('/tmp/compute.db');
var compute = require('../')(db, { path: './blobs' });

compute.on('create', function (key) {
    console.log('created', key);
});

compute.on('start', function (key, ps) {
    console.log('started', key);
});

compute.on('result', function (key, id) {
    console.log('result', key, id);
});

compute.create('sleep 5; date');

compute.run();
