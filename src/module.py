import pandas as pd

def assign_id(df, col, starting_id):
    """
    Assign unique IDs to NaN in df.col using index + starting_id
    :param df: dataframe
    :param col: column in df where the id's will be assigned to
    :param starting_id: the starting id number taken from the max of exisiting tables' id or member_id
    """

    df_index_series = df.index.to_series()
    starting_id_series = pd.Series([starting_id]*df_index_series.size, df_index_series.index)
    final_series = starting_id_series.add(df_index_series)
    df[col] = df[col].fillna(final_series).astype(int)


def transform_datetime(df, col, time_format):
    """
    Transfrom dates in string to datetime64 format. Original dates is formatted according to timeFormat
    :param df: dataframe
    :param col: column in df where datetime transformation is needed
    :param time_format: the format of date in the original df
    """
    df[col] = pd.to_datetime(df[col], format=time_format)

#List of columns to be put into the borrower_table
borrower_cols = ['member_id', 'grade', 'sub_grade', 'emp_title', 'emp_length', 'home_ownership', 'annual_inc',
    'zip_code',	'addr_state', 'dti', 'delinq_2yrs',	'earliest_cr_line',	'inq_last_6mths',
    'mths_since_last_delinq', 'mths_since_last_record', 'open_acc',	'pub_rec',	'revol_bal', 'revol_util',
    'total_acc', 'acc_now_delinq',	'tot_coll_amt',	'tot_cur_bal',	'open_acc_6m',	'open_act_il',	'open_il_12m',
    'open_il_24m',	'mths_since_rcnt_il',	'total_bal_il',	'il_util',	'open_rv_12m',	'open_rv_24m',
    'max_bal_bc', 'all_util', 'total_rev_hi_lim', 'inq_fi',	'total_cu_tl', 'inq_last_12m', 'acc_open_past_24mths',
    'avg_cur_bal',	'bc_open_to_buy',	'bc_util',	'chargeoff_within_12_mths',	'delinq_amnt','mo_sin_old_il_acct',
    'mo_sin_old_rev_tl_op',	'mo_sin_rcnt_rev_tl_op', 'mo_sin_rcnt_tl', 'mort_acc',	'mths_since_recent_bc',
    'mths_since_recent_bc_dlq',	'mths_since_recent_inq', 'mths_since_recent_revol_delinq',
    'num_accts_ever_120_pd', 'num_actv_bc_tl',	'num_actv_rev_tl',	'num_bc_sats', 'num_bc_tl', 'num_il_tl',
    'num_op_rev_tl', 'num_rev_accts', 'num_rev_tl_bal_gt_0', 'num_sats', 'num_tl_120dpd_2m', 'num_tl_30dpd',
    'num_tl_90g_dpd_24m', 'num_tl_op_past_12m', 'pct_tl_nvr_dlq', 'percent_bc_gt_75', 'pub_rec_bankruptcies',
    'tax_liens', 'tot_hi_cred_lim',	'total_bal_ex_mort', 'total_bc_limit', 'total_il_high_credit_limit']

#List of columns to be put into the joint_table
joint_cols = ['id', 'annual_inc_joint', 'dti_joint', 'verification_status_joint', 'revol_bal_joint',
    'sec_app_earliest_cr_line',	'sec_app_inq_last_6mths', 'sec_app_mort_acc', 'sec_app_open_acc', 'sec_app_revol_util','sec_app_open_act_il', 'sec_app_num_rev_accts', 'sec_app_chargeoff_within_12_mths', 'sec_app_collections_12_mths_ex_med', 'sec_app_mths_since_last_major_derog']

#List of columns to be put into the hardship_table
hardship_cols = ['id', 'hardship_flag', 'hardship_type', 'hardship_reason', 'hardship_status', 'deferral_term',
	'hardship_amount', 'hardship_start_date', 'hardship_end_date', 'payment_plan_start_date', 'hardship_length',
    'hardship_dpd', 'hardship_loan_status',	'orig_projected_additional_accrued_interest',
    'hardship_payoff_balance_amount', 'hardship_last_payment_amount']

#List of columns to be put into the debt_settlement_table
debt_settlement_cols = ['id', 'debt_settlement_flag', 'debt_settlement_flag_date', 'settlement_status',
	'settlement_date', 'settlement_amount',	'settlement_percentage', 'settlement_term']
