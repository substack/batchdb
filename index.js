var sublevel = require('level-sublevel/bytewise');
var blobs = require('content-addressable-blob-store');
var bytewise = require('bytewise');
var inherits = require('inherits');
var through = require('through2');
var EventEmitter = require('events').EventEmitter;
var defined = require('defined');
var extend = require('xtend');
var shasum = require('shasum');

module.exports = Compute;
inherits(Compute, EventEmitter);

function Compute (db, opts) {
    if (!(this instanceof Compute)) return new Compute(db, opts);
    if (!opts) opts = {};
    this.runner = opts.run;
    
    this.db = sublevel(db, {
        keyEncoding: bytewise,
        valueEncoding: 'json'
    });
    this.store = opts.store || blobs(opts);
    this.running = {};
}

Compute.prototype.create = function (cb) {
    var self = this;
    var w = self.store.createWriteStream();
    w.once('close', function () {
        self.put([ 'job', w.key ], 0, function (err) {
            if (err) return cb && cb(err);
            self.emit('create', w.key);
            if (cb) cb(null, w.key);
        });
    });
    return w;
};

Compute.prototype.push = function (jobkey, cb) {
    var now = Date.now();
    var rows = [
        {
            type: 'pending',
            key: [ 'pending', now, jobkey ],
            value: 0
        },
        {
            type: 'pending',
            key: [ 'pending-job', jobkey, now ],
            value: 0
        }
    ];
    self.db.batch(rows, function (err) {
        if (err) cb(err)
        else {
            self.emit('push', jobkey, now);
            cb(null, jobkey, now);
        }
    });
};

Compute.prototype.add = function (cb) {
    var self = this;
    var w = self.store.createWriteStream();
    w.once('close', function () {
        var now = Date.now();
        var rows = [
            {
                type: 'pending',
                key: [ 'pending', now, w.key ],
                value: 0
            },
            {
                type: 'pending',
                key: [ 'pending-job', w.key, now ],
                value: 0
            },
            {
                type: 'put',
                key: [ 'job', w.key ],
                value: 0
            }
        ];
        self.db.batch(rows, function (err) {
            if (err) cb(err)
            else {
                self.emit('create', w.key);
                self.emit('push', w.key, now);
                if (cb) cb(null, w.key, now);
            }
        });
    });
    return w;
};

Compute.prototype.run = function () {
    var self = this;
    
    self.next(function onkey (err, key) {
        if (err) {
            self.emit('error', err);
        }
        else if (!key) {
            self.once('create', function (key, pkey) {
                onkey(null, pkey);
            });
        }
        else {
            self.exec(key, function () {
                self.next(onkey);
            });
        }
    });
};

Compute.prototype.exec = function (pkey, cb) {
    var self = this;
    
    var created = pkey[1], jobkey = pkey[2];
    var r = self.store.createReadStream({ key: jobkey });
    var w = self.store.createWriteStream();
    this.running[jobkey] = (this.running[jobkey] || 0) + 1;
    
    if (typeof self.runner !== 'function') {
        throw new Error('provided runner is not a function');
    }
    
    var start = Date.now();
    var run = self.runner(jobkey, created);
    if (!run || typeof run.pipe !== 'function') {
        self.emit('error', new Error('runner return value not a stream'));
        return;
    }
    r.pipe(run).pipe(w);
    
    w.once('close', function () {
        var end = Date.now();
        if (-- self.running[jobkey] === 0) delete self.running[jobkey];
        
        self.db.batch([
            { type: 'del', key: pkey },
            { type: 'del', key: [ 'pending-job', jobkey, created ] },
            {
                type: 'put',
                key: [ 'result', jobkey, created ],
                value: { hash: w.key, start: start, end: end }
            }
        ], done);
    });
    
    function done (err) {
        if (err) return self.emit('error', err);
        self.emit('result', key, w.key);
        if (cb) cb(null, w.key);
    }
    
    self.emit('start', key);
};

Compute.prototype.next = function (cb) {
    var results = 0;
    var opts = {
        gt: [ 'pending', null ],
        lt: [ 'pending', undefined ],
        limit: 1
    };
    var s = this.db.createReadStream(opts);
    s.on('error', function (err) {
        cb(err);
        cb = function () {};
    });
    s.pipe(through.obj(write, end));
    
    function write (row, enc, next) {
        cb(null, row.key);
    }
    function end () {
        cb(null, undefined);
    }
};

Compute.prototype.list = function (type) {
    var self = this;
    var opts = {
        gt: [ 'job', null ],
        lt: [ 'job', undefined ]
    };
    return self.db.createReadStream(opts)
        .pipe(through.obj(function f (row, enc, next) {
            this.push(extend(row.value, {
                running: defined(self.running[row.key[2]], false),
                key: row.key[1],
                created: row.key[2]
            }));
            next();
        }))
    ;
};
