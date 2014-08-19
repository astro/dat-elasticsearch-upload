var Transform = require('stream').Transform;
var Writable = require('stream').Writable;
var util = require('util');
var through2 = require('through2');
var ProgressBar = require('progress');

util.inherits(BatchBuffer, Transform);
function BatchBuffer(batchSize) {
    Transform.call(this, { objectMode: true });
    this._readableState.highWaterMark = 1;
    this._writableState.highWaterMark = batchSize;
    this.batchSize = batchSize;
    this.buffer = [];
}

BatchBuffer.prototype._transform = function(chunk, encoding, callback) {
    this.buffer.push(chunk);
    this.canFlush(false);
    callback();
};

BatchBuffer.prototype._flush = function(callback) {
    this.canFlush(true);
    callback();
};

BatchBuffer.prototype.canFlush = function(force) {
    while((force && this.buffer.length > 0) || this.buffer.length >= this.batchSize) {
        var chunks = this.buffer.slice(0, this.batchSize);
        this.buffer = this.buffer.slice(this.batchSize);
        this.push(chunks);
    }
};


util.inherits(ToES, Writable);
function ToES(esclient, opts) {
    Writable.call(this, { objectMode: true, highWaterMark: this.concurrency });
    this.esclient = esclient;
    this.opts = opts;
    this.count = 0;
    this.pending = 0;
    this.concurrency = 4;
}

ToES.prototype._write = function(batch, encoding, callback) {
    if (batch.length < 1)
        return callback();

    var opts = this.opts;
    body = [];
    batch.forEach(function(doc) {
        if (doc.source) {
            /* Explicit index/type/id format */
            body.push({
                index: {
                    _index: doc.index,
                    _type: doc.type,
                    _id: doc.id
                }
            });
            body.push(doc.source);
        } else {
            /* Implicit format, guess about id */
            // TODO: get id from dat?
            body.push({
                index: {
                    _id: doc.id
                }
            });
            body.push(doc);
        }
    });

    this.pending++;

    this.esclient.bulk({
        index: opts.index,
        type: opts.type,
        body: body,
        requestTimeout: 120000,
        consistency: 'one',
        replication: 'async'
    }, function(err) {
        this.pending--;
        if (this.nextCallback) {
            var cb = this.nextCallback;
            delete this.nextCallback;
            cb(err);
        }
    }.bind(this));
    this.count += batch.length;

    if (this.pending < this.concurrency) {
        /* Ask for more */
        callback();
    } else {
        /* Wait */
        this.nextCallback = callback;
    }
};

module.exports = function(dat, esclient, opts) {
    if (opts.noProgress) {
        run(dat, esclient, function() {}, opts);
    } else {
        dat.getRowCount(function(err, total) {
            if (err) {
                console.error(err);
                return;
            }

            var progressGrain = opts.progressGrain || 1000;
            var progress = new ProgressBar(":percent [:bar] :eta", { total: Math.floor(total / progressGrain) });
            var ticks = 0;
            var tick = function() {
                ticks++;
                if (ticks >= progressGrain) {
                    progress.tick();
                    ticks = 0;
                }
            };
            run(dat, esclient, tick, opts);
        });
    }
};

function run(dat, esclient, tick, opts) {
    var stream = dat.createReadStream();
    if (opts.transform) {
        stream = stream.pipe(opts.transform);
    }

    stream = stream.pipe(through2.obj(function(obj, enc, cb) {
        this.push(obj);
        cb();
        tick();
    })).pipe(new BatchBuffer(64))
        .pipe(new ToES(esclient, opts));
};
