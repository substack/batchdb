var sublevel = require('level-sublevel/bytewise');
var blobs = require('content-addressable-blob-store');
var bytewise = require('bytewise');
var inherits = require('inherits');
var through = require('through2');
var EventEmitter = require('events').EventEmitter;
var defined = require('defined');
var extend = require('xtend');

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
                type: 'put',
                key: [ 'pending', now, w.key ],
                value: 0
            },
            {
                type: 'put',
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
            if (err) return cb(err)
            self.emit('create', w.key);
            self.emit('push', w.key, now);
            if (cb) cb(null, w.key, now);
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
    
    if (!this.running[jobkey]) this.running[jobkey] = [];
    this.running[jobkey].push(created);
    
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
        var ix = self.running[jobkey].indexOf(created);
        if (ix >= 0) self.running[jobkey].splice(ix, 1);
        if (self.running[jobkey].length === 0) delete self.running[jobkey];
        
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
        self.emit('result', jobkey, w.key, created);
        if (cb) cb(null, w.key);
    }
    
    self.emit('start', jobkey, created);
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

Compute.prototype.jobs = function (xopts) {
    var self = this;
    var opts = {
        gt: [ 'job', null ],
        lt: [ 'job', undefined ]
    };
    return self.db.createReadStream(opts)
        .pipe(through.obj(function (row, enc, next) {
            this.push({
                key: row.key[1],
                running: self.running[row.key[1]] || []
            });
            next();
        }))
    ;
};

Compute.prototype.pending = function (jkey, xopts) {
    var self = this;
    if (typeof jkey === 'object') {
        xopts = jkey;
        jkey = undefined;
    }
    var opts = jkey
        ? {
            gt: [ 'pending-job', jkey, null ],
            lt: [ 'pending-job', jkey, undefined ]
        }
        : {
            gt: [ 'pending', null ],
            lt: [ 'pending', undefined ]
        }
    ;
    return self.db.createReadStream(opts)
        .pipe(through.obj(function (row, enc, next) {
            var created = row.key[1], jobkey = row.key[2];
            var running = self.running[jobkey];
            this.push({
                job: jobkey,
                created: created,
                running: Boolean(running && running[created])
            });
            next();
        }))
    ;
};

Compute.prototype.results = function (xopts) {
    var self = this;
    if (!xopts) xopts = {};
    
    var opts = {
        gt: [ 'result', null ],
        lt: [ 'result', undefined ]
    };
    return self.db.createReadStream(opts)
        .pipe(through.obj(function (row, enc, next) {
            this.push({
                key: row.key.slice(1),
                value: extend(row.value, {
                    created: row.key[2]
                })
            });
            next();
        }))
    ;
};

Compute.prototype.getJob = function (jobkey) {
    return this.store.createReadStream({ key: jobkey });
};

Compute.prototype.getResult = function (rkey) {
    return this.store.createReadStream({ key: rkey });
};
