#!/bin/python
import sys
import pandas as pd
from datetime import datetime
from module import *
from sqlalchemy import create_engine
import os


def main(inputData):
    """
    Read data into a Pandas dataframe.
    Index missing fields of 'id' and 'member_id'
    Transform dates to datetime64 format
    Persist into database
    """

    df = pd.read_csv(inputData, low_memory=False)

    #Determine the ID range. From EDA, we know the two columns 'id' and 'member_id' are completely
    #missing, and unique ID values need to be added in-place
    idRange = len(df.index)
    colToAssignId = ['id', 'member_id']
    for col in colToAssignId:
        assignId(df, col)

    #Transform dates in string to datetime64
    colToTransDatetime = ['issue_d', 'earliest_cr_line', 'last_pymnt_d', 'next_pymnt_d', 'last_credit_pull_d', 'sec_app_earliest_cr_line']
    timeFormat = '%b-%Y'
    for col in colToTransDatetime:
        transformDatetime(df, col, timeFormat)


    #Write to database
    engine = create_engine('postgresql://'+os.environ['POSTGRESQL_USER']+':'+os.environ['POSTGRESQL_PASSWORD']+'@'+os.environ['POSTGRESQL_HOST_IP']+':'+os.environ['POSTGRESQL_PORT']+'/'+os.environ['POSTGRESQL_DATABASE'],echo=False)

    df.to_sql(os.environ['POSTGRESQL_TABLE'], engine, chunksize=10000, if_exists = 'replace')


if __name__ == '__main__':
    """
    Check for the input data passed as an argument to this script.
    Print program run time
    """

    assert len(sys.argv) == 2, 'Usage: python main.py ${INPUT_DATA}'
    inputData  = sys.argv[1]

    startTime = datetime.now()

    main(inputData)

    print(datetime.now() - startTime)
