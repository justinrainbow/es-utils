module.exports = Exporter;

var request      = require('request');
var http         = require('http');
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

  var self = this;

  this.host    = options.host;
  this.port    = options.port || 9200;
  if (this.host.indexOf(':') > 0) {
    this.host.replace(/^(.+):(\d+)$/, function (m, host, port) {
      self.port = port;
      self.host = host;
    });
  }

  this.idx     = options.index;
  this.baseUrl = 'http://' + this.host;
  this.size    = options.batchSize || 1000;
  this.delay   = options.delay || 10;
  this.query   = options.query || { match_all: {} };

  EventEmitter.call(this);
}

util.inherits(Exporter, EventEmitter);

Exporter.prototype.prepare = function (cb) {
  var self = this,
      params = {
        search_type: 'scan',
        scroll:      '5m',
        size:        this.size
      },
      opts = {
        hostname: this.host,
        port:     this.port,
        path:     '/' + this.idx + '/_search?' + qs.stringify(params),
        method:   'POST'
      },
      req;

  req = http.request(opts, function (res) {
    res.setEncoding('utf8');
    res
      .pipe(es.wait())
      .pipe(es.parse())
      .on('data', function (data) {
        cb(undefined, self.createSearch(data));
      })
      .on('error', cb);
  });

  req.write(JSON.stringify({ query: this.query }));

  req.end();
};

Exporter.prototype.createSearch = function (res) {
  assert.string(res._scroll_id, 'Unable to create the search');

  var url = this.url('_search/scroll', {
    scroll: '5m',
    scroll_id: res._scroll_id
  });

  return new ScrollSearch(url, res.hits.total, this.delay);
};


Exporter.prototype.url = function (path, opts) {
  return 'http://' + this.host + ':' + this.port + '/' + path + '?' + qs.stringify(opts);
};

function ScrollSearch(url, total, delay) {
  this.url   = url;
  this.reqId = 0;
  this.total = total;
  this.delay = delay || 10;

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
        if (self.delay > 0) {
          setTimeout(self.next.bind(self), self.delay);
        } else {
          self.next();
        }
      } else {
        self.emit('end');
      }
    });
};


