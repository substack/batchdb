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

Compute.prototype.create = function (meta, cb) {
    var self = this;
    if (typeof meta === 'function') {
        cb = meta;
        meta = {};
    }
    if (!meta) meta = {};
    var last, frac = 1;
    
    var w = self.store.createWriteStream();
    w.once('close', function () {
        var now = Date.now();
        if (last === now) {
            last = now;
            frac /= 2;
            now += frac;
        }
        else {
            last = now;
            frac = 1;
        }
        
        var rank = defined(meta.rank, now);
        var pkey = [ 'pending', rank, w.key, now ];
        var jkey = [ 'job', w.key, now ];
        
        self.db.batch([
            { type: 'put', key: pkey, value: 0 },
            { type: 'put', key: jkey, value: {} }
        ], function (err) {
            self.emit('create', w.key, pkey);
            if (cb) cb(err);
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
            self.start(key, function () {
                self.next(onkey);
            });
        }
    });
};

Compute.prototype.start = function (pkey, cb) {
    var key = pkey[2];
    var self = this;
    var r = self.store.createReadStream({ key: key });
    var w = self.store.createWriteStream();
    this.running[key] = (this.running[key] || 0) + 1;
    
    if (typeof self.runner !== 'function') {
        throw new Error('provided runner is not a function');
    }
    
    var start = Date.now();
    var run = self.runner(key);
    if (!run || typeof run.pipe !== 'function') {
        self.emit('error', new Error('runner return value not a stream'));
        return;
    }
    r.pipe(run).pipe(w);
    
    w.once('close', function () {
        var end = Date.now();
        if (-- self.running[key] === 0) delete self.running[key];
        
        self.db.batch([
            { type: 'del', key: pkey },
            {
                type: 'put',
                key: [ 'job', key, pkey[3] ],
                value: { result: w.key }
            },
            {
                type: 'put',
                key: [ 'result', key, w.key ],
                value: { start: start, end: end }
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
