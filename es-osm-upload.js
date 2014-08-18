var esclient = require('pelias-esclient')();
var datESUpload = require('./index');
var through2 = require('through2');

var dat = require('dat')(process.argv[2] || ".", ready);
function ready(err) {
    if (err) {
        console.error(err);
        return;
    }

    datESUpload(dat, esclient, {
        transform: through2.obj(function(el, enc, cb) {
            el.info.timestamp = new Date(el.info.timestamp);
            if (el.lat && el.lon) {
                // This is what Kibana displays in bettermap
                el.center = [el.lon, el.lat];
            }
            this.push(el);
            cb();
        })
    });
}
