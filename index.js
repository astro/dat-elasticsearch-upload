var ESBackend = require('geopipes-elasticsearch-backend');
var through2 = require('through2');
var ProgressBar = require('progress');

module.exports = function(dat, esclient, opts) {
    var elasticsearch = new ESBackend(esclient, 'osm');

    if (opts.noProgress) {
        run(dat, elasticsearch, function() {}, opts);
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
            run(dat, elasticsearch, tick, opts);
        });
    }
};

function run(dat, elasticsearch, tick, opts) {
    var stream = dat.createReadStream();
    if (opts.transform) {
        stream = stream.pipe(opts.transform);
    }

    stream = stream.pipe(through2.obj(function(chunk, enc, cb) {
        chunk._type = chunk.type;
        this.push(chunk);
        cb();
        tick();
    })).pipe(elasticsearch.createPullStream());
};
