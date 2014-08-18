var esclient = require('pelias-esclient')();
var ESBackend = require('geopipes-elasticsearch-backend');
var elasticsearch = new ESBackend(esclient, 'osm');
var through2 = require('through2');
var ProgressBar = require('progress');

var PROGRESS_GRAIN = 1000;

var dat = require('dat')(process.argv[2] || ".", function(err) {
    if (err) {
        console.error(err);
        return;
    }

    dat.getRowCount(function(err, total) {
        if (err) {
            console.error(err);
            return;
        }

        var stream = dat.createReadStream();
        var elementFilter = false;
        if (elementFilter) {
            stream = stream.pipe();
        }

        var progress = new ProgressBar(":percent [:bar] :eta", { total: Math.floor(total / PROGRESS_GRAIN) });
        var grain = 0;
        stream = stream.pipe(through2.obj(function(chunk, enc, cb) {
            chunk._type = chunk.type;
            this.push(chunk);

            grain++;
            if (grain == PROGRESS_GRAIN) {
                progress.tick();
                grain = 0;
            }
            cb();
        })).pipe(elasticsearch.createPullStream());
    });
});

