import pandas as pd
from collections import OrderedDict

def concatenate_header_values(df):
    for col in df.columns:
            val_1 = df.at[1, col]
            val_2 = df.at[2, col]
            if isinstance(val_1, str) and isinstance(val_2, str):
                df.at[1, col] = val_1 + ' ' + val_2
    return df

def drop_unnecessary_rows(df):
    df = df.drop([2, 3, 4, 5])
    # Find the rows to drop based on conditions
    rows_to_drop = df.index[2:][df.iloc[2:, 0].isna()]
    # Drop those rows
    df.drop(rows_to_drop, inplace=True)
    return df

def set_values_based_on_rows(df):
    col_name = df.columns[2]
    row_1_value = df.loc[0, col_name]
    df.iloc[2:, df.columns.get_loc('GroupNames')] = row_1_value
    df = df.reset_index(drop=True)
    return df

def process_ordered_set(df):
    row_1_values = df.loc[0].tolist()
    ordered_set = list(OrderedDict.fromkeys(row_1_values))
    ordered_set = [item for item in ordered_set if isinstance(item, str)]
    return ordered_set

def handle_datetime_rows(df):
    datetime_rows = df[df[0].apply(lambda x: isinstance(x, pd.Timestamp))].copy()
    new_df = df.copy()
    ordered_set = process_ordered_set(df)
    for group_name in ordered_set[2:]:
        temp_df = datetime_rows.copy()
        temp_df['GroupNames'] = group_name
        new_df = new_df.append(temp_df, ignore_index=True)
    return new_df

def replace_redundant_values(df):
    for col in df.columns[2:]:
        first_row_value = df.loc[0, col]
        for index, row in df[2:].iterrows():
            if first_row_value != row['GroupNames']:
                df.loc[index, col] = ''
    return df

def assign_new_header(df):
    df = df.drop(0)
    df.columns = df.iloc[0]
    df = df.drop(1)
    df = df.reset_index(drop=True)
    return df

def rename_and_format_columns(df):
    df.columns.values[0] = 'DateTime'
    df['DateTime'] = df['DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df.columns.values[1] = 'GroupName'
    df.columns = df.columns.str.replace('\n', ' ')
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9]', '_', regex=True)
    return df

def concatenate_df_values(df):
    def concatenate_values(x):
        return x.apply(lambda y: ''.join(y.dropna().astype(str)), axis=1)

    result_df = pd.DataFrame(index=df.index)
    for col_name in df.columns.unique():
        sub_df = df.loc[:, df.columns == col_name]
        result_df[col_name] = concatenate_values(sub_df)
    return result_df