var sublevel = require('level-sublevel/bytewise');
var blobs = require('content-addressable-blob-store');
var bytewise = require('bytewise');
var inherits = require('inherits');
var through = require('through2');
var EventEmitter = require('events').EventEmitter;
var defined = require('defined');
var spawn = require('child_process').spawn;
var multiplex = require('multiplex');
var parseShell = require('shell-quote').parse;

var defaultShell = /^win/.test(process.platform) ? 'cmd' : 'sh';

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
    
    this.shell = defined(opts.shell, process.env.SHELL, defaultShell);
    if (typeof this.shell === 'string') {
        this.shell = parseShell(this.shell);
    }
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
    w.once('close', function () {
        var now = Date.now();
        var rank = defined(meta.rank, now);
        
        self.db.batch([
            { type: 'put', key: [ 'pending', rank, w.key, now ], value: 0 }
        ], function (err) {
            self.emit('create', w.key);
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
            self.start(key, function () {
                self.next(onkey);
            });
        }
    });
};

Compute.prototype.start = function (key, cb) {
    var self = this;
    var sh = self.store.createReadStream({ key: key });
    var ps = spawn(self.shell[0], self.shell.slice(1));
    sh.pipe(ps.stdin);
    
    var w = self.store.createWriteStream();
    var m = multiplex();
    m.pipe(w);
    
    w.once('close', function () {
        self.db.batch([
            { type: 'del', key: key },
            { type: 'put', key: [ 'result', key, w.key ], value: 0 }
        ], done);
    });
    ps.stdout.pipe(m.createStream(1));
    ps.stderr.pipe(m.createStream(2));
    
    var pending = 2;
    function onend () { if (--pending === 0) m.end() }
    ps.stdout.once('end', onend);
    ps.stderr.once('end', onend);
    
    function done () {
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
        cb(null, row.key[2]);
    }
    function end () {
        cb(null, undefined);
    }
};
