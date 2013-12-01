module.exports = Exporter;

var request      = require('request');
var assert       = require('assert-plus');
var qs           = require('querystring');
var fs           = require('fs');
var es           = require('event-stream');
var EventEmitter = require('events').EventEmitter;
var util         = require('util');
var JSONStream   = require('JSONStream');

function Exporter(options) {
  assert.string(options.host, 'Missing required "host" option');
  assert.string(options.index, 'Missing required "index" option');

  this.host    = options.host;
  this.idx     = options.index;
  this.baseUrl = 'http://' + this.host;
  this.size    = options.batchSize || 1000;
  this.delay   = options.delay || 100;

  EventEmitter.call(this);
}

util.inherits(Exporter, EventEmitter);

Exporter.prototype.prepare = function (cb) {
  var self = this,
      opts = {
        search_type: 'scan',
        scroll:      '5m',
        size:        this.size
      };
  request(
    this.baseUrl + '/' + this.idx + '/_search?' + qs.stringify(opts),
    function (err, res, body) {
      if (err) {
        cb(err);
      } else {
        try {
          cb(undefined, self.createSearch(JSON.parse(body)));
        } catch (e) {
          self.emit('error', e);
        }
      }
    });
};

Exporter.prototype.createSearch = function (res) {
  assert.string(res._scroll_id, 'Unable to create the search');

  var url = this.url('_search/scroll', {
    scroll: '5m',
    scroll_id: res._scroll_id
  });

  return new ScrollSearch(url, res.hits.total);
};

function ScrollSearch(url, total) {
  this.url   = url;
  this.reqId = 0;
  this.total = total;

  EventEmitter.call(this);
}

util.inherits(ScrollSearch, EventEmitter);

ScrollSearch.prototype.next = function () {
  var self = this,
      hits = 0;

  request(this.url)
    .pipe(JSONStream.parse(['hits', 'hits', true]))
    .on('data', function (data) {
      if (Array.isArray(data)) {
        if (data.length) {
          data.forEach(function (d) {
            self.emit('data', d);

            hits ++;
          });
        }
        return;
      }

      self.emit('data', data);

      hits ++;
    })
    .on('end', function () {
      if (hits > 0) {
        self.next();
      } else {
        self.emit('done');
      }
    });
};


Exporter.prototype.url = function (path, opts) {
  return 'http://' + this.host + '/' + path + '?' + qs.stringify(opts);
};


