from pathlib import Path
import pandas as pd


# disable SettingWithCopyWarning
pd.options.mode.chained_assignment = None  # default='warn'


def one_to_many_relationship_builder(df1: pd.DataFrame, df2: pd.DataFrame, where_index_col: str, index_col_name: str) -> pd.DataFrame:
    """
    Matches values from df1 and df2 and creates a column to be inserted in the specified dataframe.

    :param df1: 'left' dataframe
    :param df2: 'right' dataframe
    :param where_index_col: 'left' | 'right' - if specified 'left' swap dfs
    :param index_col_name: name of the column with indices
    :return: column with indices
    """
    if where_index_col == 'left':
        # swap dataframes
        df = df1
        df1 = df2
        df2 = df

    func = lambda d, r: d.index[d["common_id"] == r["common_id"]].tolist()

    return pd.DataFrame({index_col_name: [func(df1, row)[0] if func(df1, row) else None for _, row in df2.iterrows()]})


def columns_to_id(df: pd.DataFrame, common_columns: list[str]) -> pd.DataFrame:
    """
    Covert two columns 'student_name' (or any string type) and 'date' (or any list 3 or more elements long) to id-like
    value. E.g. student_name = John Wick, date = [5, 10, 2022] -> 'john wick 102022'
    :param df: input dataframes with column specified in common_columns
    :param common_columns: two-element list of columns to be used to build id
    :return: id-like value e.g. 'john wick 102022'
    """
    if len(common_columns) >= 2:
        return df[common_columns[0]].apply(lambda s: s.lower()) + ' ' + df[common_columns[1]].apply(lambda l: str(l[1]) + str(l[2]))
    else:
        return df[common_columns[0]].apply(lambda s: s.lower())


def hard_reset_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Execute reset_index() if reset column not present
    """
    if 'index' in df.keys().tolist():
        df = df.drop(columns=['index'])

    return df.reset_index(drop=True).reset_index()


def many_to_many_relationship(
        df: pd.DataFrame,
        composite_junction_df: pd.DataFrame,
        col_name: str,
        index_col_names: list[str],
        common_columns: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Creates a junction table from provided dataframes.

    :param df: 'left' dataframe
    :param composite_junction_df: 'right' dataframe
    :param col_name: column in the composite_junction_df that needs to be moved to the right_df
    :param index_col_names: list of two strings with names of columns in the junction_df
    :param common_columns: columns that will be used to match rows in df and composite_junction_df
    :return: df, junction_df and right_df
    """
    right_df = hard_reset_index(composite_junction_df[[col_name]].drop_duplicates())
    composite_junction_df = hard_reset_index(composite_junction_df)

    # merge the columns for entry identification into one string - avoid variable is not hashable error
    df["common_id"] = columns_to_id(df, common_columns)
    composite_junction_df["common_id"] = columns_to_id(composite_junction_df, common_columns)

    func = lambda d, r, col: d.index[d[col] == r[col]].tolist()

    junction_df = pd.DataFrame({
        index_col_names[0]: [func(df, row, "common_id")[0] if func(df, row, "common_id") else None for _, row in composite_junction_df.iterrows()],
        index_col_names[1]: [func(right_df, row, col_name)[0] if func(right_df, row, col_name) else None for _, row in composite_junction_df.iterrows()]
    })

    df.drop(columns=["common_id"], inplace=True)

    return df, junction_df, right_df


def df_relationship_builder(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        common_columns: list[str],
        relationship_type: str,
        index_column_name: str,
        *, where_index_column: str = "left"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Based on common columns matches rows from two dataframes
    :param left_df: ...
    :param right_df: ...
    :param common_columns: columns that will be used to match rows in left and right dataframe
    :param relationship_type: "1-to-1" | "1-to-many" | "0-or-1-to-many" | "0-or-1-to-1"
    :param index_column_name: name of the new column with indices
    :param where_index_column: "left" | "right" <- only considered when relationship_type = "1-to-1"
    :return: ...
    """
    # reset indices (primary key)
    left_df = hard_reset_index(left_df)
    right_df = hard_reset_index(right_df)

    # merge the columns for entry identification into one string - avoid variable is not hashable error
    left_df["common_id"] = columns_to_id(left_df, common_columns)
    right_df["common_id"] = columns_to_id(right_df, common_columns)

    if relationship_type == "1-to-1":
        if where_index_column == "right":
            right_df[index_column_name] = left_df[left_df["common_id"] == right_df["common_id"]].index
        else:
            left_df[index_column_name] = right_df[right_df["common_id"] == left_df["common_id"]].index
    else:  # "1-to-many" | "0-or-1-to-many" | "0-or-1-to-1", where_index_column = "right"
        right_df[index_column_name] = one_to_many_relationship_builder(left_df, right_df, "right", index_column_name)

    left_df.drop(columns=["common_id"], inplace=True)
    right_df.drop(columns=["common_id"], inplace=True)

    return left_df, right_df


if __name__ == "__main__":
    from academy_csv import AcademyCSV
    from talent_csv import TalentCSV
    from talent_json import TalentJSON

    # import pickles
    pickle_jar_path = Path(__file__).parent.parent.parent.resolve() / "pickle_jar"
    raw_academy_csv_df = pd.read_pickle(pickle_jar_path / "academy_csv_v2.pkl")
    raw_talent_csv_df = pd.read_pickle(pickle_jar_path / "talent_csv_v2.pkl")
    raw_talent_json_df = pd.read_pickle(pickle_jar_path / "talent_json.pkl")

    # instantiate transform classes
    academy_csv_transform = AcademyCSV(raw_academy_csv_df)
    talent_csv_transform = TalentCSV()
    talent_json_transform = TalentJSON(raw_talent_json_df)

    # split and clean each dataframe
    trainer_df, course_df, academy_performance_df = academy_csv_transform.transform_academy_csv()
    student_information_df, invitation_df = talent_csv_transform.transform_talent_csv(raw_talent_csv_df)
    (trainee_performance_df, weakness_junction_df, strength_junction_df,
     tech_self_score_junction_df) = talent_json_transform.transform_talent_json()

    df_relationship_builder(student_information_df, invitation_df, ["student_name", "date"], "1-to-1", "invitation_id")

    weakness_junction_df = weakness_junction_df[weakness_junction_df.weakness.notnull()]
    strength_junction_df = strength_junction_df[strength_junction_df.strength.notnull()]
    tech_self_score_junction_df = tech_self_score_junction_df[tech_self_score_junction_df.tech_self_score.notnull()]

    many_to_many_relationship(student_information_df, weakness_junction_df, "weakness", ["student_information_id", "weakness_id"], ["student_name", "date"])
