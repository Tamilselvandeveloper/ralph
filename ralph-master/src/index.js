const _ = require('lodash');
const csv = require('csv');
const fs = require('fs-extra');
const path = require('path');
const loki = require('lokijs');
const TSTCOL = 'tst';
const SRCCOL = 'src';
const RESCOL = 'results';
const jiff = require('jiff');
const lokiattrs = ['meta', '$loki'];
const os = require('os');
const commander = require('commander');

commander
    .version('1.0.0')
    .option('-l --location [l]', 'Enter the location to load files from.', path.join(__dirname, '/../test'))
    .option('-s --source [s]', 'Enter the source file')
    .option('-t --test [t]', 'Enter the target file')
    .option('-e --entity [trd | cpty | bonddefn]', 'Enter the entity')
    .parse(process.argv);

if (!commander.source || !commander.test || !commander.entity) {
    commander.outputHelp();
    return;
}
const recSrcFile = path.join(commander.location, commander.source); //The file against which to be compared
const recTestFile = path.join(commander.location, commander.test); //The file to test against src file
const recCfg = conventionalize(require('../cfg/' + commander.entity));
const recdb = new loki(recCfg.id + '_recon.db');

fs.ensureDir(path.join(__dirname, '../build/'));

function conventionalize(cfg) {
    var conCfg = {};
    conCfg.id = cfg.id;
    conCfg.primaryKey = {
        recSrcFile: _.camelCase(cfg.primaryKey.recSrcFile),
        recTestFile: _.camelCase(cfg.primaryKey.recTestFile)
    };

    //If the mapping is specified as a string, because the columns are the same
    if (_.isString(cfg.primaryKey)) {
        conCfg.primaryKey = {
            recSrcFile: _.camelCase(cfg.primaryKey),
            recTestFile: _.camelCase(cfg.primaryKey)
        };
    }

    conCfg.mappings = _.map(cfg.mappings, el => {
        var cEl = {};
        _.keys(el).forEach((v, i) => {
            if (v === cfg.primaryKey || v === cfg.primaryKey.recSrcFile) return;
            //If its the first, then camelcase it, others just clone it
            if (i === 0) {
                _.set(cEl, 'src', _.camelCase(v));
                _.set(cEl, 'tst', _.camelCase(_.get(el, v)));
            } else {
                _.set(cEl, v, _.get(el, v));
            }
        });

        return cEl;
    });

    return conCfg;
}

console.log(recSrcFile, recTestFile);
const parserOptions = {
    trim: true,
    columns: function(cols) {
        return _.map(cols, function(column) {
            return _.camelCase(column);
        });
    }
};

var srcparser = csv.parse(parserOptions, (err, data) => {
    if (err) {
        console.error('Error occurred while parsing source data', err);
        return err;
    }
    loadData(data, recCfg, SRCCOL);
});

var tstparser = csv.parse(parserOptions, (err, data) => {
    if (err) {
        console.error('Error occurred while parsing test data', err);
        return err;
    }
    loadData(data, recCfg, TSTCOL);
});

function loadData(data, recCfg, type) {
    var col = recdb.addCollection(type);
    data.forEach(row => {
        col.insert(row);
    });
    recdb.saveDatabase();
}

function copyRow(row, newAttribs) {
    return _.omit(_.merge(row, newAttribs), lokiattrs);
}

function runRecon(recCfg) {
    if (!recCfg.status.sourceCompleted) {
        console.log('Waiting for source to be loaded');
        return;
    }
    if (!recCfg.status.testCompleted) {
        console.log('Waiting for test to be loaded');
        return;
    }
    console.log('Running reconciliation...');
    var reccol = recdb.addCollection(RESCOL);
    recdb
        .getCollection(SRCCOL)
        .find()
        .forEach(row => {
            //By default, put it as MISSING, if there is a MATCH, it will either show up as MATCHED/MISMATCHED
            reccol.insert(copyRow(row, { recStatus: 'MISSING', primaryKey: row[recCfg.primaryKey.recSrcFile] }));
        });
    recdb
        .getCollection(TSTCOL)
        .find()
        .forEach(tstRow => {
            var srcRow = recdb.getCollection(RESCOL).findOne(_.set({}, recCfg.primaryKey.recSrcFile, tstRow[recCfg.primaryKey.recTestFile]));
            if (srcRow) {
                if (srcRow.recStatus === 'MATCHED' || srcRow.recStatus === 'MISMATCH') {
                    //Check for duplicates and mark them
                    reccol.insert(copyRow(tstRow, { recStatus: 'DUPLICATE', primaryKey: tstRow[recCfg.primaryKey.recTestFile] }));
                } else {
                    srcRow.recStatus = 'MATCHED';
                    //Go through all mappings and find if they are equal
                    recCfg.mappings.forEach(mpg => {
                        if (_.trim(srcRow[mpg.src]) != _.trim(tstRow[mpg.tst])) {
                            if (!mpg.diffExpected) srcRow.recStatus = 'MISMATCH';
                            srcRow[mpg.src] = {
                                srcValue: srcRow[mpg.src],
                                tstValue: tstRow[mpg.tst]
                            };
                        }
                    });
                    reccol.update(srcRow);
                }
            } else {
                //Record not found, so new record
                reccol.insert(copyRow(tstRow, { recStatus: 'ADDED', primaryKey: tstRow[recCfg.primaryKey.recTestFile] }));
            }
        });
    console.log('Generating output...');
    var cnt = 0;
    var hdrsWritten = false;
    const outputPath = path.join(__dirname, '../build/' + recCfg.id + '_recresults.csv');
    recdb
        .getCollection(RESCOL)
        .find()
        .forEach(row => {
            if (!hdrsWritten) {
                fs.writeFileSync(outputPath, orderedString(row, 'k'));
                hdrsWritten = true;
            }
            fs.writeFileSync(outputPath, orderedString(row, 'v'), { flag: 'a' });
            cnt++;
        });
    console.log('Total rec count %d', cnt);
}

function orderedString(obj, type) {
    //Compacting to avoid empty column names
    var ks = _.compact(
        _.concat(
            ['recStatus', 'primaryKey'],
            recCfg.mappings.map(mpg => {
                return mpg.src;
            })
        )
    );
    if (type === 'k') {
        return ks.join(',') + os.EOL;
    } else {
        var vs = [];
        ks.forEach(key => {
            if (!key) return;
            var v = obj[key];
            if (_.isObject(v)) {
                v = v.srcValue + ' | ' + v.tstValue;
            }
            vs.push('"' + ('' + v).replace('"', '"') + '"');
        });
        return vs.join(',') + os.EOL;
    }
}

fs
    .createReadStream(recSrcFile)
    .pipe(srcparser)
    .on('end', function() {
        console.log('Finished loading source file');
        _.set(recCfg, 'status.sourceCompleted', true);
        runRecon(recCfg);
    });
fs
    .createReadStream(recTestFile)
    .pipe(tstparser)
    .on('end', function() {
        console.log('Finished loading test file');
        _.set(recCfg, 'status.testCompleted', true);
        runRecon(recCfg);
    });
