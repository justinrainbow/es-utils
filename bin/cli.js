#!/usr/bin/env node
var program     = require('commander');
var assert      = require('assert-plus');
var util        = require('util');
var fs          = require('fs');
var ProgressBar = require('progress');
var multimeter  = require('multimeter');
var Exporter    = require('../lib/exporter');
var Indexer     = require('../lib/indexer');
var es          = require('event-stream');
var zlib        = require('zlib');

program.name = 'es';

program
  .option('-f, --from <host>', 'Source host', parseHost)
  .option('-t, --to [host]', 'Target host', parseHost)
  .option('-q, --quiet', 'Quiet time', Boolean);


program
  .command('copy <index>')
  .option('-s, --size [size]', 'Number of records per request', Number, 1000)
  .option('-d, --delay [delay]', 'Time to wait between requests (in ms)', Number, 100)
  .option('-r, --rename [rename]', 'Rename the index during import', String)
  .option('-o, --output [file]', 'Filename to write the exported JSON data to', String)
  .option('--query [query]', 'Optionally limit the results to only those matching this query', String)
  .option('-c, --compress [number]', 'Compress the output', Number, 9)
  .action(function (idx, cmd) {
    if (cmd.query) {
      try {
        cmd.query = JSON.parse(cmd.query);
      } catch (e) {
        console.error('Unable to parse the provided query');
        console.error(e.stack);
        process.exit(1);
      }
    }

    var ex = new Exporter({
      host:      program.from,
      index:     idx,
      batchSize: cmd.size,
      delay:     cmd.delay,
      query:     cmd.query
    });

    var multi;

    if (! program.quiet) {
      multi = multimeter(process);
      multi.on('^C', process.exit);
      multi.charm.reset();

      multi.write(util.format(
        'Exporting %s from %s\n',
        idx,
        program.from
      ));
    }

    var bars = [];

    ex.prepare(function (err, search) {

      exportProgress();

      if (cmd.output) {
        fileProgress();
      }

      if (program.to) {
        importProgress();
      }

      function exportProgress() {
        var bar, cnt = 0;

        if (! program.quiet) {
          multi.write('export:\n');

          bar = multi(10, 2 + bars.length, {
            width: 50,
            solid: {
              text: '>',
              foreground: 'green'
            },
            empty: {
              text: ' '
            }
          });

          bars.push(bar);
        }

        search
          .on('data', function () {
            cnt ++;

            bar && bar.ratio(cnt, search.total);
          })
          .on('end', function () {
            console.log('\n');
          });
      }

      function fileProgress() {
        var file = fs.createWriteStream(cmd.output),
            str = '',
            bar,
            cnt = 0;

        if (! program.quiet) {
          multi.write('file:\n');

          bar = multi(10, 2 + bars.length, {
            width: 50,
            solid: {
              text: '|',
              foreground: 'white',
              background: 'cyan'
            },
            empty: {
              text: ' '
            }
          });

          bars.push(bar);
        }

        var data = search
          .pipe(es.stringify())
          .pipe(es.through(function (doc) {
            str += doc;

            if (cnt % 1000 === 0) {
              this.emit('data', str);
              str = '';
            }
            cnt ++;

            bar && bar.ratio(cnt, search.total);
          }, function () {
            this.emit('data', str);
            str = '';
            this.emit('end');
          }));

        if (cmd.compress) {
          data = data.pipe(zlib.createGzip({ compress: cmd.compress }));
        }

        data
          .pipe(file)
          .on('finish', function () {
            if (! program.quiet) {
              multi.destroy();
            }
          });
      }

      function importProgress() {
        var bar,
            cnt = 0,
            failures = [],
            indexer = new Indexer({
              host: program.to,
              batchSize: cmd.size
            });

        if (! program.quiet) {
          multi.write('import:\n');

          bar = multi(10, 2 + bars.length, {
            width: 50,
            solid: {
              text: '+',
              foreground: 'yellow'
            },
            empty: {
              text: ' '
            }
          });

          bars.push(bar);
        }

        indexer
          .on('data', function (data) {
            var op = data.index;

            if (op.ok !== true) {
              failures.push(op);
            }

            cnt ++;

            bar && bar.ratio(cnt, search.total, util.format('%d / %d (%d failed)', cnt, search.total, failures.length));
          });

        process.on('exit', function () {
          if (failures.length) {
            console.log('The following docs failed to import');
            console.log(JSON.stringify(failures, null, 2));
          }
        });

        search
          .on('data', function (doc) {
            if (cmd.rename) {
              doc._index = cmd.rename;
            }
            indexer.write(doc);
          })
          .on('end', function () {
            indexer.end();
          });
      }
    });
  });

program
  .command('import <file>')
  .option('-s, --size [size]', 'Number of records per request', Number, 1000)
  .action(function (file, cmd) {
    var indexer = new Indexer({
      host:      program.to,
      batchSize: cmd.size
    });

    var totalRead = 0,
        totalIndexed = 0,
        failures = [];

    var multi = multimeter(process);
    multi.on('^C', process.exit);
    multi.charm.reset();

    multi.write(util.format(
      'Importing %s to %s\n',
      file,
      program.to
    ));


    multi.write('import:\n');

    var bar = multi(10, 2, {
      width: 50,
      solid: {
        text: '+',
        foreground: 'yellow'
      },
      empty: {
        text: ' '
      }
    });

    indexer
      .on('data', function (data) {
        var op = data.index;

        if (op.ok !== true) {
          failures.push(op);
        }

        totalIndexed ++;

        update();
      });

    process.on('exit', function () {
      if (failures.length) {
        console.log('The following docs failed to import');
        console.log(JSON.stringify(failures, null, 2));
      }
    });

    fs.createReadStream(file)
      .pipe(es.split())
      .pipe(es.parse())
      .on('data', function (data) {
        totalRead ++;
        indexer.write(data);
        update();
      })
      .on('end', function () {
        indexer.end();
      });

    function update() {
      bar.ratio(totalIndexed, totalRead, util.format('%d / %d (%d failures)', totalIndexed, totalRead, failures.length));
    }
  })


function parseHost(host) {
  var parts = String(host).split(':');
  if (parts.length < 2) {
    return host + ':9200';
  }
  return host;
}

program.parse(process.argv);

