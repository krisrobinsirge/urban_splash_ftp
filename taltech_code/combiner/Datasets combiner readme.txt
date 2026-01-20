Purpose: Combine cleaned Observator and ColiMinder CSVs into period outputs and a cumulative general file.
Script: combine.py (repository root). Uses pandas.
Setup

Requires Python 3.8+ and pandas installed.
Default folders: data_in (inputs), data_out (outputs).
Input expectations

Files live in data_in.
Must have matching date ranges:
cleaned_data_Observator_<from>to<to>.csv
cleaned_data_ColiMinder_<from>to<to>.csv
Only processed when both files exist for the same <from>to<to>.
QC “PASS/FAIL” columns remain in inputs but are dropped from outputs.
Output files (written to data_out)

Per period: cleaned_and_combined_data_<from>to<to>.csv
Cumulative: cleaned_and_combined_data_general.csv (unit row + sorted data rows)
Column structure

Matches the provided sample combined file. Columns (in order):
TimeStamp, Origin, Activity - Coliminder, BGA PC RFU, BGA PC ug/L, Chlorophyll RFU, Chlorophyll ug/L, Cond uS/cm, fDOM QSU, fDOM RFU, microFlu_TRP, pH, Signal strength, SpCond uS/cm, Temp C, Turbidity
First row is a unit row (copied from the sample).
Row rules

Observator rows: fill Observator sensor columns; leave Coliminder-only fields empty; Origin = “Observator”.
ColiMinder rows: fill Activity; leave Observator sensor columns empty; Origin = “Coliminder”.
All outputs sorted by TimeStamp.
General file de-duplication

When appending new period data to cleaned_and_combined_data_general.csv, duplicates on (TimeStamp, Origin) keep the existing row and ignore the new one.
General file always keeps the unit row on top and stays TimeStamp-sorted.
How to run

Default: python combine.py
Custom folders: python combine.py --input data_in --output data_out
Processing flow

Discover matching Observator/ColiMinder file pairs by date range.
Load and clean each pair; drop QC columns; align to target column order; add origin labels.
Write period-specific combined CSV to data_out.
Append into cleaned_and_combined_data_general.csv with de-duplication and sorting.
Notes

If you add more date ranges, just place the new cleaned files into data_in and rerun the script.
If filenames change, update the patterns in combine.py (regex near the top).