import pandas as pd

def assignId(df, col):
    """
    Assign unique IDs to NaN in df.col using index
    """
    df[col] = df[col].fillna(df.index.to_series()).astype(int)


def transformDatetime(df, col, timeFormat):
    """
    Transfrom dates in string to datetime64 format. Original dates is formatted according to timeFormat
    """
    df[col] = pd.to_datetime(df[col], format=timeFormat)
