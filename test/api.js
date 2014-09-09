var test = require('tape');
var batchdb = require('../');
var level = require('level')
var path = require('path');
var concat = require('concat-stream');
var duplexer = require('duplexer2');
var through = require('through2');
var mkdirp = require('mkdirp');

var tmpdir = path.join(
    require('osenv').tmpdir(),
    'batchdb-test-' + Math.random()
);
mkdirp.sync(tmpdir);

test('api', function (t) {
    t.plan(8);
    
    var db = level(path.join(tmpdir, 'db'));
    var compute = batchdb(db, {
        path: path.join(tmpdir, 'blob'),
        run: function () {
            var input = concat(function (body) {
                if (body.toString('utf8') === 'fail') {
                    dup.emit('error', new Error('yo'));
                    return;
                }
                
                t.equal(body.toString('utf8'), 'robot');
                output.end('beep boop\n');
            });
            var output = through();
            var dup = duplexer(input, output);
            return dup;
        }
    });
    
    compute.on('result', function (key, id) {
        compute.get(key).pipe(concat(function (body) {
            t.equal(body.toString('utf8'), 'robot');
        }));
        compute.get(id).pipe(concat(function (body) {
            t.equal(body.toString('utf8'), 'beep boop\n');
        }));
        
        var results = [];
        compute.list('result').pipe(through.obj(
            function (row, enc, next) {
                results.push(row);
                next();
            },
            function () {
                t.equal(results.length, 1);
                t.equal(results[0].value.hash, id);
            }
        ));
    });
    
    compute.add().end('robot');
    compute.add().end('fail');
    
    compute.on('fail', function (err) {
        t.equal(err.message, 'yo');
        var results = [];
        compute.list('fail').pipe(through.obj(write, end));
        function write (row, enc, next) {
            results.push(row);
            next();
        }
        function end () {
            t.equal(results.length, 1);
            t.deepEqual(results[0].value, { message: 'yo' });
        }
    });
    
    compute.run();
});
