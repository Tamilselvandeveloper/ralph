const _ = require('lodash');
const csv = require('csv');
const fs = require('fs-extra');
const path = require('path');
const loki = require('lokijs');
const logger = require('./logger');
const os = require('os');

const swpGen31 = path.join(__dirname, '../test/31SwapGenerators.csv'); //The file against which to be compared
const swpGen211 = path.join(__dirname, '../test/211SwapGenerators.csv'); //The file to test against src file
const mappings = require('../../migration/src/trd/mappings');
const fileName = './build/mappedfile.csv';
//const recCfg = conventionalize(require('../cfg/trd'));

const parseOptions = {
    trim: true,
    columns: cols => {
        return _.map(cols, column => {
            return _.camelCase(column);
        });
    }
};

const compareAttributes = [
    'leg1Type',
    'leg2Type',
    'leg1Currency',
    'leg2Currency',
    'leg1Calculation',
    'leg2Calculation',
    'leg1PaymentFreq',
    'leg2PaymentFreq',
    'leg1Convention',
    'leg2Convention',
    'leg1BusinessDays',
    'leg2BusinessDays',
    'leg1CalcShifter',
    'leg2CalcShifter'
];

var customGenerators = [];

var mapped = [];

fs.createReadStream(swpGen31).pipe(
    csv.parse(parseOptions, (err, newGenerators) => {
        //Remove the pricing curves
        _.remove(newGenerators, ngn => {
            return ngn.swapTemplate.startsWith('\\');
        });
        //Remove the pricing curves
        customGenerators = _.remove(newGenerators, ngn => {
            if (ngn.swapTemplate.endsWith('CUSTOM')) {
                if (ngn.leg1Currency != ngn.leg2Currency) {
                    throw new Error('Invalid state, cannot have CUSTOM generator with two different currencies');
                }
                ngn.customCurrency = ngn.leg1Currency; //Can there be different currencies?
                ngn.customIndex = ngn.leg1Currency; //Can there be different currencies?
                return true;
            }
            return false;
        });
        //_.remove(newGenerators, customGenerators);
        logger.info('Total new generators: %s custom, %s non-custom', customGenerators.length, newGenerators.length);
        fs.createReadStream(swpGen211).pipe(
            csv.parse(parseOptions, (err, oldGenerators) => {
                logger.info('Parsing %s trades', oldGenerators.length);
                oldGenerators.forEach(trd => {
                    trd.leg1Calculation = trd.compoundingFormula1 != '' ? 'CMP' : trd.leg1Calculation;
                    trd.leg2Calculation = trd.compoundingFormula2 != '' ? 'CMP' : trd.leg2Calculation;

                    var actTrd = _.cloneDeep(trd);
                    //Map to new values
                    _.set(trd, 'payCal1', mappings.mapCalendar(_.get(trd, 'payCal1')));
                    _.set(trd, 'payCal2', mappings.mapCalendar(_.get(trd, 'payCal2')));
                    _.set(trd, 'startDelay1', mappings.mapDateShifter(_.get(trd, 'startDelay1')));
                    _.set(trd, 'startDelay2', mappings.mapDateShifter(_.get(trd, 'startDelay1')));
                    _.set(trd, 'schDed1', mappings.mapDateShifter(_.get(trd, 'schDed1')));
                    _.set(trd, 'schDed2', mappings.mapDateShifter(_.get(trd, 'schDed1')));
                    _.set(trd, 'leg1Convention', mappings.mapRateConvention(_.get(trd, 'leg1Convention')));
                    _.set(trd, 'leg2Convention', mappings.mapRateConvention(_.get(trd, 'leg2Convention')));
                    _.set(trd, 'leg1Schedule', mappings.mapScheduleGenerator(_.get(trd, 'leg1Schedule')));
                    _.set(trd, 'leg2Schedule', mappings.mapScheduleGenerator(_.get(trd, 'leg2Schedule')));
                    _.set(trd, 'leg1Index', mappings.mapIndexLabel(_.get(trd, 'leg1Index')));
                    _.set(trd, 'leg2Index', mappings.mapIndexLabel(_.get(trd, 'leg2Index')));

                    var possibleGenerators = _.filter(newGenerators, ngn => {
                        return _.isEqual(
                            _.sortBy(_.concat(_.values(_.pick(ngn, compareAttributes)), getIndices(ngn))),
                            _.sortBy(_.concat(_.values(_.pick(trd, compareAttributes)), getIndices(trd)))
                        );
                    });

                    if (addMapped(possibleGenerators, actTrd)) {
                        return;
                    }

                    logger.info('Unable to find a perfect match for %s, so looking to see the closest match', trd.mNb);
                    var continueSearching = true;
                    var reducedAttribs = _.clone(compareAttributes);
                    do {
                        reducedAttribs = _.slice(reducedAttribs, 0, reducedAttribs.length - 2);
                        possibleGenerators = _.filter(newGenerators, ngn => {
                            return _.isEqual(
                                _.sortBy(_.concat(_.values(_.pick(ngn, reducedAttribs)), getIndices(ngn))),
                                _.sortBy(_.concat(_.values(_.pick(trd, reducedAttribs)), getIndices(trd)))
                            );
                        });
                        if (possibleGenerators.length === 1) {
                            logger.info('Found a proposed match for %s, with attributes %s', trd.mNb, reducedAttribs);
                            //found one match
                            continueSearching = false;
                            _.set(actTrd, 'proposedGenerator', possibleGenerators[0].swapTemplate);
                            mapped.push(actTrd);
                        } else if (reducedAttribs.length === 8) {
                            //Unable to get any match, so exit by setting to custom
                            continueSearching = false;
                            _.set(actTrd, 'customGenerator', getCustomGenerator(trd));
                            mapped.push(actTrd);
                        }
                    } while (continueSearching);

                    //Find a match

                    /*possibleGenerators = _.filter(possibleGenerators, ngn => {
                        return _.isEqual(_.sortBy(getPaymentFrequency(ngn)), _.sortBy(getPaymentFrequency(trd)));
                    });
                    if (addMapped(possibleGenerators, actTrd)) {
                        return;
                    }*/

                    //Capture the first one and keep so that we can use it when there is no match
                    var broadMatch = _.slice(possibleGenerators, 0, 1);
                    //console.log(possibleGenerators, broadMatch);
                });

                var stdcols = ['mNb', 'generatorName', 'swapTemplate', 'proposedGenerator', 'customGenerator'];
                var remCols = _.chain(['mNb', 'generatorName', 'swapTemplate', 'proposedGenerator', 'customGenerator'])
                    .concat(_.keys(mapped[0]))
                    .uniq()
                    .value();
                fs.writeFileSync(fileName, remCols.join(',') + os.EOL);

                mapped.forEach(row => {
                    var str = '';
                    remCols.forEach(col => {
                        str += (row[col] || '') + ',';
                    });
                    fs.appendFileSync(fileName, str + os.EOL);
                    //logger.info(row.mNb, row.swapTemplate);
                });
            })
        );
    })
);

function addMapped(possibleGenerators, actTrd) {
    if (possibleGenerators.length === 1) {
        _.set(actTrd, 'swapTemplate', possibleGenerators[0].swapTemplate);
        mapped.push(actTrd);
        return true;
    }
}

function getPaymentShifter(trd) {
    var fllegs = getLegAttributes(trd, 'Float');
    return _.map(fllegs, 'paymentShifter');
}

function getPaymentFrequency(trd) {
    var fxlegs = getLegAttributes(trd, 'Fixed');
    return _.map(fxlegs, 'paymentFreq');
}

function getCustomGenerator(trd) {
    var indices = getIndices(trd);
    if (!indices) return 'CUSTOM';
    var index = indices[0];

    if (index.indexOf('CAD') != -1) {
        if (index.indexOf('CDOR') != -1) {
            return 'CAD CDOR CUSTOM';
        } else {
            return 'CAD CORRA CUSTOM';
        }
    } else if (index.indexOf('USD') != -1) {
        if (index.indexOf('LIB') != -1) {
            return 'USD LIBOR CUSTOM';
        } else if (index.indexOf('BMA') != -1) {
            return 'USD SIFMA CUSTOM';
        } else {
            return 'USD FEDFUNDS CUSTOM';
        }
    } else {
        return 'EUR EURIBOR CUSTOM';
    }
}

function getLegAttributes(trd, type) {
    var legs = [];
    if (trd.leg1Type === type) {
        legs.push({
            type: trd.leg1Type,
            payRec: trd.leg1PayRec,
            currency: trd.leg1Currency,
            index: trd.leg1Index,
            convention: trd.leg1Convention,
            schedule: trd.leg1Schedule,
            businessDays: trd.leg1BusinessDays,
            paymentShifter: trd.leg1Paymentshifter,
            paymentFreq: trd.leg1PaymentFreq,
            calculation: trd.leg1Calculation,
            calendar: trd.payCal1,
            startDelay: trd.startDelay1,
            schDed: trd.schDed1,
            compounding: trd.leg1Compounding
        });
    }
    if (trd.leg2Type === type) {
        legs.push({
            type: trd.leg2Type,
            payRec: trd.leg2PayRec,
            currency: trd.leg2Currency,
            index: trd.leg2Index,
            convention: trd.leg2Convention,
            schedule: trd.leg2Schedule,
            businessDays: trd.leg2BusinessDays,
            paymentShifter: trd.leg2Paymentshifter,
            paymentFreq: trd.leg2PaymentFreq,
            calculation: trd.leg2Calculation,
            calendar: trd.payCal2,
            startDelay: trd.startDelay2,
            schDed: trd.schDed2,
            compounding: trd.leg2Compounding
        });
    }
    return legs;
}

function getIndices(trd) {
    var indices = [];
    if (trd.leg1Type === 'Float') {
        indices.push(trd.leg1Index);
    }
    if (trd.leg2Type === 'Float') {
        indices.push(trd.leg2Index);
    }
    return indices;
}
