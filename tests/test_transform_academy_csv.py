import unittest
from pathlib import Path
import pandas as pd
from src.transform_toolbox.academy_csv import AcademyCSV
from src.transform_toolbox.date_format_test import date_format_test


class TestTransformAcademyCSV(unittest.TestCase):
    def setUp(self) -> None:
        self.pickle_jar_path = Path(__file__).resolve().parent.parent / "pickle_jar"
        self.raw_df = pd.read_pickle(self.pickle_jar_path / "academy_csv_v2.pkl")
        self.academy_csv_transform = AcademyCSV(self.raw_df)
        self.trainer_df, self.course_df, self.academy_performance_df = self.academy_csv_transform.transform_academy_csv()
        self.dt = pd.DataFrame({
            'float': [1.0],
            'int': [1],
            'other': ['foo'],
            'bool': [True]
        })

    ############################
    #   TESTING COLUMN NAMES   #
    ############################

    def test_transform_academy_csv_trainer_df_col_names(self) -> None:
        expected = {"trainer_name"}
        actual = set(self.trainer_df.columns.tolist())
        self.assertEqual(expected, actual)

    def test_transform_academy_csv_course_df_col_names(self) -> None:
        expected = {"course_name", "trainer_name", "date"}
        actual = set(self.course_df.columns.tolist())
        self.assertEqual(expected, actual)

    def test_transform_academy_csv_academy_performance_df_col_names(self) -> None:
        expected = {"student_name", "date", "course_name", "analytic", "independent", "determined", "professional",
                    "studious", "imaginative", "week"}
        actual = set(self.academy_performance_df.columns.tolist())
        self.assertEqual(expected, actual)

    ################################
    #   TESTING COLUMN DATATYPES   #
    ################################

    def test_transform_academy_csv_trainer_df_datatype(self) -> None:
        expected = {self.dt['other'].dtype}
        actual = set(self.trainer_df.dtypes.tolist())
        self.assertEqual(expected, actual)

    def test_transform_academy_csv_course_df_datatype(self) -> None:
        expected = {self.dt['other'].dtype}
        actual = set(self.course_df.dtypes.tolist())
        self.assertEqual(expected, actual)

    def test_transform_academy_csv_academy_performance_df_datatype(self) -> None:
        expected = {self.dt['other'].dtype, self.dt['int'].dtype}
        actual = set(self.academy_performance_df.dtypes.tolist())
        self.assertEqual(expected, actual)

    ###############################
    #   CHECKING FOR DUPLICATES   #
    ###############################

    def test_transform_academy_csv_trainer_df_duplicates(self) -> None:
        actual = self.trainer_df.duplicated().tolist()
        self.assertTrue(not all(actual))

    def test_transform_academy_csv_course_df_duplicates(self) -> None:
        actual = self.course_df
        actual["date"] = actual["date"].apply(lambda x: tuple(x))
        actual = actual.duplicated().tolist()
        self.assertTrue(not all(actual))

    def test_transform_academy_csv_academy_performance_df_duplicates(self) -> None:
        actual = self.academy_performance_df
        actual["date"] = actual["date"].apply(lambda x: tuple(x))
        actual = actual.duplicated().tolist()
        self.assertTrue(not all(actual))

    #########################################
    #   TESTING FORMATTING OF EACH COLUMN   #
    #########################################

    def test_transform_academy_csv_col_format_person_name(self) -> None:
        list_actual = [
            self.trainer_df["trainer_name"],
            self.course_df["trainer_name"],
            self.academy_performance_df["student_name"]
        ]
        for actual in list_actual:
            self.assertTrue(all([a.istitle() for a in actual if not pd.isnull(a)]))
            self.assertTrue(all([a.count('  ') == 0 for a in actual if not pd.isnull(a)]))

    def test_transform_academy_csv_col_format_course_name(self) -> None:
        list_actual = [
            self.course_df["course_name"],
            self.academy_performance_df["course_name"]
        ]
        for actual in list_actual:
            for row in actual:
                if row:
                    self.assertTrue(row.count('  ') == 0,
                                    f"Duplicate spaces in entry {row} in {actual.name}. row.count('  ') = {row.count('  ')}")
                    for word in row.split(' '):
                        if len(word) > 2:
                            self.assertTrue(
                                (word[0].isupper() or word[1].isupper()) and not
                                (word[0].isupper() and word[1].isupper()),
                                f"Word '{word}' in entry '{row}' in {actual.name}."
                            )

    def test_transform_academy_csv_col_format_date(self) -> None:
        actual_list = [self.course_df["date"], self.academy_performance_df["date"]]
        for actual in actual_list:
            date_format_test(actual)


if __name__ == '__main__':
    unittest.main()
