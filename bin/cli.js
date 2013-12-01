#!/usr/bin/env node
var program     = require('commander');
var assert      = require('assert-plus');
var util        = require('util');
var fs          = require('fs');
var ProgressBar = require('progress');
var multimeter  = require('multimeter');
var Exporter    = require('../lib/exporter');
var Indexer     = require('../lib/indexer');


program.name = 'es';

program
  .option('-f, --from <host>', 'Source host', parseHost)
  .option('-t, --to [host]', 'Target host', parseHost);


program
  .command('copy <index>')
  .option('-s, --size [size]', 'Number of records per request', Number, 1000)
  .option('-d, --delay [delay]', 'Time to wait between requests (in ms)', Number, 100)
  .option('-r, --rename [rename]', 'Rename the index during import', String)
  .option('-o, --output [file]', 'Filename to write the exported JSON data to', String)
  .action(function (idx, cmd) {
    var ex = new Exporter({
      host:      program.from,
      index:     idx,
      batchSize: cmd.size,
      delay:     cmd.delay
    });

    var multi = multimeter(process);
    multi.on('^C', process.exit);
    multi.charm.reset();

    multi.write(util.format(
      'Exporting %s from %s\n',
      idx,
      program.from
    ));

    var bars = [];

    ex.prepare(function (err, search) {

      exportProgress();

      if (cmd.output) {
        fileProgress();
      }

      if (program.to) {
        importProgress();
      }

      search.next();

      function exportProgress() {
        var bar, cnt = 0;

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

        search
          .on('data', function () {
            cnt ++;

            bar.ratio(cnt, search.total);
          })
          .on('end', function () {
            console.log('\n');
          });
      }

      function fileProgress() {
        var file = fs.createWriteStream(cmd.output),
            bar,
            cnt = 0;

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

        search
          .on('data', function (doc) {
            file.write(JSON.stringify(doc) + '\n', 'utf8', function () {
              cnt ++;

              bar.ratio(cnt, search.total);
            });
          })
          .on('end', function () {
            file.end();
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

        indexer
          .on('data', function (data) {
            var op = data.index;

            if (op.ok !== true) {
              failures.push(op);
            }

            cnt ++;

            bar.ratio(cnt, search.total, util.format('%d / %d (%d failed)', cnt, search.total, failures.length));
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


function parseHost(host) {
  var parts = String(host).split(':');
  if (parts.length < 2) {
    return host + ':9200';
  }
  return host;
}

program.parse(process.argv);

