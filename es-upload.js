#!/usr/bin/env node
var esclient = require('pelias-esclient')();
var datESUpload = require('./index');

var dat = require('dat')(process.argv[2] || ".", ready);
function ready(err) {
    if (err) {
        console.error(err);
        return;
    }

    datESUpload(dat, esclient, { index: process.argv[3] || "dat", type: process.argv[4] || "item" });
}
