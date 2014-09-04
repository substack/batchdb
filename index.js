var sublevel = require('level-sublevel/bytewise');
var blobs = require('content-addressable-blob-store');
var bytewise = require('bytewise');
var inherits = require('inherits');
var through = require('through2');
var EventEmitter = require('events').EventEmitter;
var defined = require('defined');

module.exports = Compute;
inherits(Compute, EventEmitter);

function Compute (db, opts) {
    if (!(this instanceof Compute)) return new Compute(db, opts);
    if (!opts) opts = {};
    
    if (typeof opts.run !== 'function') {
        throw new Error('opts.run parameter required');
    }
    this.runner = opts.run;
    
    this.db = sublevel(db, {
        keyEncoding: bytewise,
        valueEncoding: 'json'
    });
    this.store = opts.store || blobs(opts);
}

Compute.prototype.create = function (meta, cb) {
    var self = this;
    if (typeof meta === 'function') {
        cb = meta;
        meta = {};
    }
    if (!meta) meta = {};
    
    var w = self.store.createWriteStream();
    w.once('close', function () {
        var now = Date.now();
        var rank = defined(meta.rank, now);
        var pkey = [ 'pending', rank, w.key, now ];
        
        self.db.batch([
            { type: 'put', key: pkey, value: 0 },
        ], function (err) {
            self.emit('create', w.key, pkey);
            if (cb) cb(err);
        });
    });
    return w;
};

Compute.prototype.run = function () {
    var self = this;
    self.running = true;
    
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
    
    var run = self.runner(key);
    if (!run || typeof run.pipe !== 'function') {
        self.emit('error', new Error('runner return value not a stream'));
        return;
    }
    r.pipe(run).pipe(w);
    
    w.once('close', function () {
        self.db.batch([
            { type: 'del', key: pkey },
            { type: 'put', key: [ 'result', key, w.key ], value: 0 }
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
