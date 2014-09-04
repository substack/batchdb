var sublevel = require('level-sublevel/bytewise');
var blobs = require('content-addressable-blob-store');
var bytewise = require('bytewise');
var inherits = require('inherits');
var through = require('through2');
var EventEmitter = require('events').EventEmitter;
var extend = require('xtend');
var defined = require('defined');

module.exports = Compute;
inherits(Compute, EventEmitter);

function Compute (db, opts) {
    if (!(this instanceof Compute)) return new Compute(db, opts);
    if (!opts) opts = {};
    
    this.db = sublevel(db, {
        keyEncoding: bytewise,
        valueEncoding: 'json'
    });
    this.store = blobs(opts);
}

Compute.prototype.create = function (sh, meta, cb) {
    var self = this;
    if (typeof sh === 'object') {
        cb = meta;
        meta = sh;
        sh = null;
    }
    if (typeof sh === 'function') {
        cb = sh;
        meta = {};
        sh = null;
    }
    if (!meta) meta = {};
    
    var w = self.store.createWriteStream();
    w.on('close', function () {
        var now = Date.now();
        var job = extend({ date: now }, meta);
        var rank = defined(job.rank, now);
        
        self.db.batch([
            { type: 'put', key: [ 'job', w.key ], value: job },
            { type: 'put', key: [ 'pending', rank, w.key, now ], value: 0 }
        ], function (err) {
            self.emit('create', w.key, job);
            if (cb) cb(err);
        });
    });
    if (sh) w.end(sh);
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
            self.once('create', function (key) { onkey(null, key) });
        }
        else {
            console.log(err, key);
        }
    });
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
        cb(null, row.key[2]);
    }
    function end () {
        cb(null, undefined);
    }
};
