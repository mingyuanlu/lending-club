#!/bin/bash

source setup_db_properties.sh

python src/main.py data/loan-funded-amnt-lt-2500.csv
