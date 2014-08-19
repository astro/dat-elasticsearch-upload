#!/usr/bin/env node
var esclient = require('pelias-esclient')();
var datESUpload = require('./index');
var through2 = require('through2');

var INDEX_NAME = 'osm';
var ES_TYPE_MAPPING = {
    _id: { path: 'id' },
    _all: { enabled: true },
    date_detection: false,
    properties: {
        location : { type : "geo_shape", precision: "50m" },
        center: { type: 'geo_point', lat_lon: true },
        // This is what Kibana displays in bettermap
        center_lon_lat: { type: 'string', store: true, index: 'no' },
        timestamp: { type: 'date' }
    }
};
var ES_SCHEMA = {
    mappings: {
        node: ES_TYPE_MAPPING,
        way: ES_TYPE_MAPPING,
        relation: ES_TYPE_MAPPING
    }
};

var dat = require('dat')(process.argv[2] || ".", ready);
function ready(err) {
    if (err) {
        console.error(err);
        return;
    }

    esclient.indices.create( {
        index: INDEX_NAME,
        body: ES_SCHEMA
    }, function(err) {
        if (err) {
            console.error(err);
            return;
        }

        datESUpload(dat, esclient, {
            index: INDEX_NAME,
            transform: through2.obj(function(el, enc, cb) {
                el.info.timestamp = new Date(el.info.timestamp);
                var push = function() {
                    if (el.lat && el.lon) {
                        el.center = {
                            lon: el.lon,
                            lat: el.lat
                        };
                    }
                    if (el.center) {
                        el.center_lon_lat = [el.center.lon, el.center.lat];
                    }
                    this.push(el);
                    cb();
                }.bind(this);

                if (el.type === 'node') {
                    el.location = {
                        type: 'point',
                        coordinates: [el.lon, el.lat]
                    };
                    push();
                } else if (el.type === 'way') {
                    var t1 = Date.now();
                    fetchWayNodes(dat, el, function(err, nodes) {
                        var t2 = Date.now();
                        console.log("Got", el.refs.length, "in", (t2 - t1), "ms");
                        nodes = nodes || [];
                        var lat = 0, lon = 0, points = [];
                        nodes.forEach(function(node) {
                            lat += node.lat;
                            lon += node.lon;
                            points.push([node.lon, node.lat]);
                        });
                        el.lat = lat / nodes.length;
                        el.lon = lon / nodes.length;
                        var isArea = points.length > 3 &&
                            points[0] === points[points.length - 1];
                        el.location = {
                            type: isArea ? 'polygon' : 'linestring',
                            coordinates: isArea ? [points] : points
                        };
                        push();
                    });
                } else {
                    // TODO: build location field for relations
                    push();
                }
            })
        });
    });
}

function fetchWayNodes(dat, way, cb) {
    var refs = way.refs || [];
    var i = 0;
    var results = [];
    function go() {
        if (i < refs.length) {
            dat.get(refs[i], function(err, node) {
                if (err) {
                    console.error(err);
                } else {
                    results.push(node);
                }
                i++;
                go();
            });
        } else {
            cb(null, results);
        }
    }
    go();
}
