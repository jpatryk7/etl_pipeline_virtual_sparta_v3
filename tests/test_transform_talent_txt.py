import unittest
from pathlib import Path
import pandas as pd
from src.transform_toolbox.talent_txt import TalentTXT
from src.transform_toolbox.date_format_test import date_format_test


class TestTransformAcademyCSV(unittest.TestCase):
    def setUp(self) -> None:
        self.pickle_jar_path = Path(__file__).resolve().parent.parent / "pickle_jar"
        self.raw_df = pd.read_pickle(self.pickle_jar_path / "talent_txt_v2.pkl")
        self.talent_txt_transform = TalentTXT()
        self.test_score_df = self.talent_txt_transform.transform_talent_txt(self.raw_df)
        self.dt = pd.DataFrame({'float': [1.0], 'int': [1], 'other': ['foo'], 'bool': [True]})

    ############################
    #   TESTING COLUMN NAMES   #
    ############################

    def test_transform_talent_txt_test_score_df_col_names(self) -> None:
        expected = {"student_name", "date", "psychometrics", "presentation"}
        actual = set(self.test_score_df.columns.tolist())
        self.assertEqual(expected, actual)

    ################################
    #   TESTING COLUMN DATATYPES   #
    ################################

    def test_transform_talent_txt_test_score_df_datatype(self) -> None:
        expected = {self.dt['other'].dtype}
        actual = set(self.test_score_df.dtypes.tolist())
        self.assertEqual(expected, actual)

    ###############################
    #   CHECKING FOR DUPLICATES   #
    ###############################

    def test_transform_talent_txt_test_score_df_duplicates(self) -> None:
        actual = self.test_score_df
        actual["date"] = actual["date"].apply(lambda x: tuple(x))
        actual = actual.duplicated().tolist()
        self.assertTrue(not all(actual))

    #########################################
    #   TESTING FORMATTING OF EACH COLUMN   #
    #########################################

    def test_transform_talent_txt_col_format_person_name(self) -> None:
        actual = self.test_score_df["student_name"]
        for row in actual:
            if row:
                self.assertTrue(row.count('  ') == 0,
                                f"Duplicate spaces in entry {row} in {actual.name}. row.count('  ') = {row.count('  ')}")
                for word in row.split(' '):
                    if len(word) > 2:
                        self.assertTrue(
                            (word[0].isupper() or word[1].isupper()) and not (word[0].isupper() and word[1].isupper()),
                            f"Word '{word}' in entry '{row}' in {actual.name}."
                        )

    def test_transform_talent_txt_col_format_date(self) -> None:
        actual = self.test_score_df["date"]
        date_format_test(actual)

    def test_transform_talent_txt_format_test_results(self) -> None:
        actual_list = [self.test_score_df[key] for key in ["psychometrics", "presentation"]]
        for actual in actual_list:
            self.assertTrue(all(['/' in a for a in actual if not pd.isnull(a)]))
            self.assertTrue(all([a.split('/')[0].isdigit() and a.split('/')[1].isdigit() for a in actual if not pd.isnull(a)]))
            self.assertTrue(all([' ' not in a for a in actual if not pd.isnull(a)]))


if __name__ == "__main__":
    unittest.main()
