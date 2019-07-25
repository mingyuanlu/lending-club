#!/bin/bash

#Set up database properties
source setup_db_properties.sh

#Run main python script
python ${LENDING_CLUB_DIR}/src/main.py $1
