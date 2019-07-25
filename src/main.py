#!/bin/python
import sys
import pandas as pd
from datetime import datetime
from module import *
from sqlalchemy import create_engine
import os


def main(input_data):
    """
    Read data into a Pandas dataframe.
    Index missing fields of 'id' and 'member_id'
    Transform dates to datetime64 format
    Persist into database
    :param input_data: input CSV file
    """

    df = pd.read_csv(input_data, low_memory=False)

    #Use SqlAlchemy engine to write to PostgreSQL
    engine = create_engine('postgresql://'
    +os.environ['POSTGRESQL_USER']+':'+os.environ['POSTGRESQL_PASSWORD']+'@'+os.environ['POSTGRESQL_HOST_IP']+':'
    +os.environ['POSTGRESQL_PORT']+'/'+os.environ['POSTGRESQL_DATABASE'],echo=False)

    #Check if the tables already exist. Since all tables are created simultaneously, simple check one of them.
    #If tables exist, gather the largest id and member_id from the exisitng tables, and assign new id's and
    #member_id's starting from them. If no existing tables can be found, simply start from 0
    id_start = 0
    member_id_start = 0
    table_exists = engine.has_table(os.environ['POSTGRESQL_TABLE']+'_loan')
    if table_exists:
        query = '''SELECT MAX(id) AS id_max, MAX(member_id) AS member_id_max FROM '''+ os.environ['POSTGRESQL_TABLE']+'_loan'+''';'''
        existing_id_df = pd.read_sql_query(query, engine)

        id_start        = existing_id_df.iloc[0]['id_max'] + 1
        member_id_start = existing_id_df.iloc[0]['member_id_max'] + 1


    #Assign id and member_id
    col_to_assign_id = ['id', 'member_id']
    id_start_dict = {col_to_assign_id[0]: id_start, col_to_assign_id[1]: member_id_start}
    for col in col_to_assign_id:
        assign_id(df, col, id_start_dict[col])


    #Transform dates in string to datetime64
    col_to_trans_datetime = ['issue_d', 'earliest_cr_line', 'last_pymnt_d', 'next_pymnt_d', 'last_credit_pull_d', 'sec_app_earliest_cr_line']
    time_format = '%b-%Y'
    for col in col_to_trans_datetime:
        transform_datetime(df, col, time_format)

    #Break the table down to normalize
    borrower_table        = df[borrower_cols]
    joint_table           = df[joint_cols]
    hardship_table        = df[hardship_cols]
    debt_settlement_table = df[debt_settlement_cols]

    #Need to keep member_id in the loan table
    cols_to_drop = list(set(borrower_cols + joint_cols + hardship_cols + debt_settlement_cols))
    cols_to_drop = [x for x in cols_to_drop if x!='member_id']
    df.drop(cols_to_drop, axis=1)


    #Write each table to database
    df.to_sql(os.environ['POSTGRESQL_TABLE']+'_loan', engine, chunksize=10000, if_exists = 'append')
    borrower_table.to_sql(os.environ['POSTGRESQL_TABLE']+'_borrower', engine, chunksize=10000, if_exists = 'append')
    joint_table.to_sql(os.environ['POSTGRESQL_TABLE']+'_joint', engine, chunksize=10000, if_exists = 'append')
    hardship_table.to_sql(os.environ['POSTGRESQL_TABLE']+'_hardship', engine, chunksize=10000, if_exists = 'append')
    debt_settlement_table.to_sql(os.environ['POSTGRESQL_TABLE']+'_debt_settlement', engine, chunksize=10000, if_exists = 'append')



if __name__ == '__main__':
    """
    Check for the input data passed as an argument to this script.
    Print program run time
    """

    assert len(sys.argv) == 2, 'Usage: python main.py ${INPUT_DATA}'
    input_data  = sys.argv[1]

    start_time = datetime.now()

    main(input_data)

    print(datetime.now() - start_time)
