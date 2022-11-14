import datetime
from pathlib import Path
import warnings
import pandas as pd
from transform_toolbox.df_relationship_builder import df_relationship_builder, many_to_many_relationship
from sqlalchemy import create_engine
from schema import tables_list, connection_list, get_value
import pprint as pp
from subprocess import call
import sys

from transform_toolbox.academy_csv import AcademyCSV
from transform_toolbox.talent_csv import TalentCSV
from transform_toolbox.talent_json import TalentJSON
from transform_toolbox.talent_txt import TalentTXT


if __name__ == "__main__":
    project_path = Path(__file__).parent.parent.resolve()
    warnings.simplefilter(action='ignore', category=FutureWarning)

    print("Restarting Docker container... ", end='')
    call(str(project_path / "restart_docker_container.sh"), shell=True)
    print("DONE")

    # import pickles
    print("Importing .pkl files... ", end='')
    pickle_jar_path = project_path / "pickle_jar"
    raw_academy_csv_df = pd.read_pickle(pickle_jar_path / "academy_csv_v2.pkl")
    raw_talent_csv_df = pd.read_pickle(pickle_jar_path / "talent_csv_v2.pkl")
    raw_talent_json_df = pd.read_pickle(pickle_jar_path / "talent_json.pkl")
    raw_talent_txt_df = pd.read_pickle(pickle_jar_path / "talent_txt_v2.pkl")
    print("DONE")

    # instantiate transform classes
    academy_csv_transform = AcademyCSV(raw_academy_csv_df)
    talent_csv_transform = TalentCSV()
    talent_json_transform = TalentJSON(raw_talent_json_df)
    talent_txt_transform = TalentTXT()

    # split and clean each dataframe
    print("Slicing and cleaning dataframes... ", end='')
    trainer_df, course_df, academy_performance_df = academy_csv_transform.transform_academy_csv()
    student_information_df, invitation_df = talent_csv_transform.transform_talent_csv(raw_talent_csv_df)
    (trainee_performance_df, weakness_junction_df, strength_junction_df,
     tech_self_score_junction_df) = talent_json_transform.transform_talent_json()
    test_score_df = talent_txt_transform.transform_talent_txt(raw_talent_txt_df)
    print("DONE")

    # build relationships between dataframes
    print("Building relationships between dataframes... (1/9)", end='')
    student_information, invitation = df_relationship_builder(
        left_df=student_information_df,
        right_df=invitation_df,
        common_columns=["student_name", "date"],
        index_column_name="invitation_id",
        relationship_type="1-to-1"
    )
    sys.stdout.write("\033[F")
    print("\rBuilding relationships between dataframes... (2/9)", end='')

    student_information, test_score = df_relationship_builder(
        left_df=student_information,
        right_df=test_score_df,
        common_columns=["student_name", "date"],
        index_column_name="student_information_id",
        relationship_type="0-or-1-to-1",
        where_index_column="right"
    )
    sys.stdout.write("\033[F")
    print("\rBuilding relationships between dataframes... (3/9)", end='')

    student_information, academy_performance = df_relationship_builder(
        left_df=student_information,
        right_df=academy_performance_df,
        common_columns=["student_name"],
        index_column_name="student_information_id",
        relationship_type="0-or-1-to-1",
        where_index_column="right"
    )
    print("\rBuilding relationships between dataframes... (4/9)", end='')

    student_information, trainee_performance = df_relationship_builder(
        left_df=student_information,
        right_df=trainee_performance_df,
        common_columns=["student_name", "date"],
        index_column_name="student_information_id",
        relationship_type="0-or-1-to-1",
        where_index_column="right"
    )
    print("\rBuilding relationships between dataframes... (5/9)", end='')

    student_information, tech_self_score_junction, tech_self_score = many_to_many_relationship(
        df=student_information,
        composite_junction_df=tech_self_score_junction_df,
        col_name="tech_self_score",
        index_col_names=["student_information_id", "tech_self_score_id"],
        common_columns=["student_name", "date"]
    )
    print("\rBuilding relationships between dataframes... (6/9)", end='')

    student_information, weakness_junction, weakness = many_to_many_relationship(
        df=student_information,
        composite_junction_df=weakness_junction_df,
        col_name="weakness",
        index_col_names=["student_information_id", "weakness_id"],
        common_columns=["student_name", "date"]
    )
    print("\rBuilding relationships between dataframes... (7/9)", end='')

    student_information, strength_junction, strength = many_to_many_relationship(
        df=student_information,
        composite_junction_df=strength_junction_df,
        col_name="strength",
        index_col_names=["student_information_id", "strength_id"],
        common_columns=["student_name", "date"]
    )
    print("\rBuilding relationships between dataframes... (8/9)", end='')

    course, academy_performance = df_relationship_builder(
        left_df=course_df,
        right_df=academy_performance,
        common_columns=["course_name", "date"],
        index_column_name="course_id",
        relationship_type="1-to-many",
        where_index_column="right"
    )
    print("\rBuilding relationships between dataframes... (9/9)", end='')

    trainer, course = df_relationship_builder(
        left_df=trainer_df,
        right_df=course,
        common_columns=["trainer_name"],
        index_column_name="trainer_id",
        relationship_type="1-to-many",
        where_index_column="right"
    )
    print("\rBuilding relationships between dataframes... DONE")

    # convert dates from list to datetime.date
    print("Convert list-type variables to datetime.date... ", end='')

    def list_to_date(arr):
        return datetime.date(arr[2], arr[1], arr[0]) if arr else None
    student_information["dob"] = student_information["dob"].apply(list_to_date)
    invitation["invited_date"] = invitation["invited_date"].apply(list_to_date)
    print("DONE")

    # set up engine for MySQL
    print("Connecting to MySQL database... ", end='')
    dialect = "mysql"
    driver = "pymysql"
    username = "root"
    password = "root"
    host = "127.0.0.1"
    port = 3306
    uri = f"{dialect}://{username}:{password}@{host}:{port}/virtual_sparta"
    engine = create_engine(uri)
    print("DONE")

    with engine.connect() as con:
        # insert tables in the database
        print("Inserting tables:")
        for i, table_name in enumerate(tables_list):
            print(f"\t({i + 1}/{len(tables_list)}) {table_name}... ", end='')
            table = locals()[table_name]
            column_names = get_value(table_name + "_col_names")
            dtypes = get_value(table_name + "_dtypes")
            table[column_names].to_sql(table_name, con=engine, index=False, dtype=dtypes)

            if "junction" not in table_name:
                con.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (`index`);")

            print("DONE")

        print("Adding foreign keys:")
        for i, (origin_table, target_table) in enumerate(connection_list):
            print(f"\t({i + 1}/{len(connection_list)}) {origin_table} <- {target_table}... ", end='')
            con.execute(f"ALTER TABLE `{origin_table}` "
                        f"ADD CONSTRAINT fk_{origin_table}_{target_table} "
                        f"FOREIGN KEY (`{target_table}_id`) REFERENCES {target_table}(`index`);")
            print("DONE")

    pp.pprint(con.execute("SHOW TABLES;").fetchall())
    for table_name in tables_list:
        pp.pprint(con.execute(f"DESCRIBE {table_name};").fetchall())
