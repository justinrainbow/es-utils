module.exports = Indexer;

var request      = require('request');
var http         = require('http');
var assert       = require('assert-plus');
var util         = require('util');
var EventEmitter = require('events').EventEmitter;
var es           = require('event-stream');

util.inherits(Indexer, EventEmitter);

function Indexer(options) {
  var self = this;

  assert.string(options.host, 'Missing required "host" option');

  this.host = options.host;
  this.port = options.port || 9200;
  if (this.host.indexOf(':') > 0) {
    this.host.replace(/^(.+):(\d+)$/, function (m, host, port) {
      self.port = port;
      self.host = host;
    });
  }
  this.path = '/_bulk';
  this.batchSize = options.batchSize || 100;
  this.docCount = 0;
  this.totalCount = 0;
};

Indexer.prototype.write = function (data) {
  var meta = {},
      doc  = {},
      str  = '';

  if (! this.req) {
    this.req = this.createRequest();
  }

  Object.getOwnPropertyNames(data).forEach(function (key) {
    if (~['_index', '_type', '_id'].indexOf(key)) {
      meta[key] = data[key];
    } else if ('_source' === key) {
      doc = data[key];
    }
  });

  str += JSON.stringify({ 'index': meta }) + '\n';
  str += JSON.stringify(doc) + '\n';

  this.req.write(str);

  this.docCount ++;
  this.totalCount ++;

  if (this.docCount >= this.batchSize) {
    this.req.end();
    delete this.req;
  }
};

Indexer.prototype.end = function () {
  if (this.req) {
    this.req.end();
  }
  this.emit('end');
};

Indexer.prototype.onResponse = function (res) {
  var self = this,
      body = '',
      data;

  res.setEncoding('utf8');
  res
    .pipe(es.wait())
    .pipe(es.parse())
    .on('data', function (data) {
      self.emit('response', data);
      if (Array.isArray(data.items)) {
        data.items.forEach(function (item) {
          self.emit('data', item);
        });
      }
    });
};

Indexer.prototype.createRequest = function () {
  var self = this,
      opts = {
        hostname: this.host,
        path:     this.path,
        port:     9200,
        method:   'POST'
      },
      req;

  req = http.request(opts, function (res) {
    self.onResponse(res);
  });
  req.setTimeout(30000, function () {
    req.abort();
  });
  this.docCount = 0;
  return req;
};
