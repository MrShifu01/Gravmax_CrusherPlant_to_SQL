import pandas as pd


def read_excel_file(file_path):
    try:
        df = pd.read_excel(file_path, header=None)
    except Exception as e:
        print(f"Could not read Excel file {file_path}: {e}")
        return
    return df

#! TEMPORARY, SPEAK TO CLIENT
def set_higher_level_labels(df):
    df.loc[0, 6:13] = 'Secondary Crusher'
    df.loc[0, 13:20] = 'Tertiary Crusher'
    return df


def drop_columns(df):
    # Indexing columns between "Time on" and "Running"
    for idx, col in enumerate(df.columns):
        if df.at[1, col] == "Time on":
            start_index = idx
        elif df.at[1, col] == "Running":
            end_index = idx
            break

    # Add columns between 'Time on' and 'Running' to cols_to_drop
    if start_index is not None and end_index is not None:
        # This should get the column names
        cols_to_drop = df.columns[start_index + 1:end_index].to_list()
        df = df.drop(columns=cols_to_drop)

    return df


def forward_fill(df):
    df.iloc[0] = df.iloc[0].fillna(method='ffill')
    df.iloc[1] = df.iloc[1].fillna(method='ffill')
    return df


def drop_redundant_datetime_column(df):
    df = df.drop(df.columns[1], axis=1)
    return df


def create_groupnames_column(df):
    df.insert(loc=1, column='GroupNames', value='')
    return df
