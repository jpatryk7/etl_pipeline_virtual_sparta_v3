import unittest
from pathlib import Path
import pandas as pd
from src.transform_toolbox.talent_json import TalentJSON
from src.transform_toolbox.date_format_test import date_format_test


class TestTransformTalentJSON(unittest.TestCase):
    def setUp(self) -> None:
        self.pickle_jar_path = Path(__file__).parent.parent.resolve() / "pickle_jar"
        self.raw_df = pd.read_pickle(self.pickle_jar_path / "talent_json.pkl")
        self.talent_json_transform = TalentJSON(self.raw_df)
        (
            self.trainee_performance_df,
            self.weakness_junction_df,
            self.strength_junction_df,
            self.tech_self_score_junction_df
        ) = self.talent_json_transform.transform_talent_json()
        self.dt = pd.DataFrame({'float': [1.0], 'int': [1], 'other': ['foo'], 'bool': [True]})

    ############################
    #   TESTING COLUMN NAMES   #
    ############################

    def test_transform_talent_json_trainee_performance_df_col_names(self) -> None:
        expected = {"student_name", "date", "self_development", "geo_flex", "financial_support", "result", "course_interest"}
        actual = set(self.trainee_performance_df.columns.tolist())
        self.assertEqual(expected, actual)

    def test_transform_talent_json_weakness_junction_df_col_names(self) -> None:
        expected = {"student_name", "date", "weakness"}
        actual = set(self.weakness_junction_df.columns.tolist())
        self.assertEqual(expected, actual)

    def test_transform_talent_json_strength_junction_df_col_names(self) -> None:
        expected = {"student_name", "date", "strength"}
        actual = set(self.strength_junction_df.columns.tolist())
        self.assertEqual(expected, actual)

    def test_transform_talent_json_tech_self_score_junction_df_col_names(self) -> None:
        expected = {"student_name", "date", "tech_self_score", "value"}
        actual = set(self.tech_self_score_junction_df.columns.tolist())
        self.assertEqual(expected, actual)

    ################################
    #   TESTING COLUMN DATATYPES   #
    ################################

    def test_transform_talent_json_trainee_performance_df_datatypes(self) -> None:
        expected = {self.dt['other'].dtype, self.dt['bool'].dtype}
        actual = set(self.trainee_performance_df.dtypes.tolist())
        self.assertEqual(expected, actual)

    def test_transform_talent_json_weakness_junction_df_datatypes(self) -> None:
        expected = {self.dt['other'].dtype}
        actual = set(self.weakness_junction_df.dtypes.tolist())
        self.assertEqual(expected, actual)

    def test_transform_talent_json_strength_junction_df_datatypes(self) -> None:
        expected = {self.dt['other'].dtype}
        actual = set(self.strength_junction_df.dtypes.tolist())
        self.assertEqual(expected, actual)

    def test_transform_talent_json_tech_self_score_junction_df_datatypes(self) -> None:
        expected = {self.dt['other'].dtype, self.dt['int'].dtype}
        actual = set(self.tech_self_score_junction_df.dtypes.tolist())
        self.assertEqual(expected, actual)

    ###############################
    #   CHECKING FOR DUPLICATES   #
    ###############################

    def test_transform_talent_json_trainee_performance_df_duplicates(self) -> None:
        actual = self.trainee_performance_df
        actual["date"] = actual["date"].apply(lambda x: tuple(x))
        actual = self.trainee_performance_df.duplicated().tolist()
        self.assertTrue(not all(actual))

    def test_transform_talent_json_weakness_junction_df_duplicates(self) -> None:
        actual = self.weakness_junction_df
        actual["date"] = actual["date"].apply(lambda x: tuple(x))
        actual = actual.duplicated().tolist()
        self.assertTrue(not all(actual))

    def test_transform_talent_json_strength_junction_df_duplicates(self) -> None:
        actual = self.strength_junction_df
        actual["date"] = actual["date"].apply(lambda x: tuple(x))
        actual = actual.duplicated().tolist()
        self.assertTrue(not all(actual))

    def test_transform_talent_json_tech_self_score_junction_df_duplicates(self) -> None:
        actual = self.tech_self_score_junction_df
        actual["date"] = actual["date"].apply(lambda x: tuple(x))
        actual = actual.duplicated().tolist()
        self.assertTrue(not all(actual))

    #########################################
    #   TESTING FORMATTING OF EACH COLUMN   #
    #########################################

    def test_transform_talent_json_col_format_person_name(self) -> None:
        list_actual = [
            self.trainee_performance_df["student_name"],
            self.trainee_performance_df["course_interest"],
            self.weakness_junction_df["student_name"],
            self.strength_junction_df["student_name"],
            self.tech_self_score_junction_df["student_name"]
        ]
        for actual in list_actual:
            for row in actual:
                if row:
                    self.assertTrue(row.count('  ') == 0, f"Duplicate spaces in entry {row} in {actual.name}. row.count('  ') = {row.count('  ')}")
                    for word in row.split(' '):
                        if len(word) > 2:
                            self.assertTrue(
                                (word[0].isupper() or word[1].isupper()) and not
                                (word[0].isupper() and word[1].isupper()),
                                f"Word '{word}' in entry '{row}' in {actual.name}."
                            )

    def test_transform_talent_json_col_format_date(self) -> None:
        actual_list = [
            self.trainee_performance_df["date"],
            self.weakness_junction_df["date"],
            self.strength_junction_df["date"],
            self.tech_self_score_junction_df["date"]
        ]
        for actual in actual_list:
            date_format_test(actual)

    def test_transform_talent_json_col_format_result(self) -> None:
        actual = self.trainee_performance_df["result"].tolist()
        self.assertTrue(all([a == "Pass" or "Fail" for a in actual if not pd.isnull(a)]))

    def test_transform_talent_json_col_format_tech_self_score_value(self) -> None:
        actual = self.tech_self_score_junction_df["value"].tolist()
        self.assertTrue(all([0 <= a for a in actual]))


if __name__ == '__main__':
    unittest.main()
