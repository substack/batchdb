var batchdb = require('../');
var spawn = require('child_process').spawn;
var duplexer = require('duplexer');

var db = require('level')('/tmp/compute.db');
var compute = batchdb(db, { path: '/tmp/compute.blobs', run: run });

function run (key) {
    var ps = spawn('bash');
    return duplexer(ps.stdin, ps.stdout);
}

compute.on('result', function (key, id) {
    console.log('RESULT', key, id);
    compute.getResult(id).pipe(process.stdout);
});

compute.run();
