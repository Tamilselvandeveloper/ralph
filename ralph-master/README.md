# ralph reconciliation bot

A bot to reconcile between a "source" file and a "target" file.

## Current Features

*   Can reconcile two text files.
*   Primary key to be defined

## Future features

*   Ability to define multiple keys as primary key

### How to run

Say if you want to reconcile the following two files `2.11_Trades_Att_UDF_03.22.18.csv` & `APP13_Trades_03.22.2018.csv`, then execute the following command

`node src/index.js -e trd -s 2.11_Trades_Att_UDF_03.22.18.csv -t APP13_Trades_03.22.2018.csv`

It will generate a csv file with the reconciliation results under the build folder.
