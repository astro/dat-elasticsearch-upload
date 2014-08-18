var esclient = require('pelias-esclient')();
var datESUpload = require('./index');

var dat = require('dat')(process.argv[2] || ".", ready);
function ready(err) {
    if (err) {
        console.error(err);
        return;
    }

    datESUpload(dat, esclient);
}
