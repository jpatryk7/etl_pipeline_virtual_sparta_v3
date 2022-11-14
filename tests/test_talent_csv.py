import unittest
from pathlib import Path
import pandas as pd
from src.transform_toolbox.talent_csv import TalentCSV


class TestTalentCSV(unittest.TestCase):
    def setUp(self) -> None:
        self.pickle_jar_path = Path(__file__).resolve().parent.parent / "pickle_jar"
        self.raw_df = pd.read_pickle(self.pickle_jar_path / "talent_csv_v2.pkl")
        self.talent_csv_transform = TalentCSV()

    def test__get_date_column(self) -> None:
        expected = [1, 8, 2015]
        actual = self.talent_csv_transform._get_date_from_string("SomeDir/August2015Whatever.txt")
        self.assertEqual(expected, actual)

    def test__transform_dob(self) -> None:
        expected = [5, 9, 1995]
        actual = self.talent_csv_transform._transform_dob("05/09/1995")
        self.assertEqual(expected, actual)

    def test__transform_invited_date(self) -> None:
        expected = [5, 9, 2019]
        actual = self.talent_csv_transform._transform_invited_date(5.0, "September 2019")
        self.assertEqual(expected, actual)

    def test__normalise_string_upper_case_input(self) -> None:
        expected = "Daniel Storm"
        actual = self.talent_csv_transform._normalise_string("DANIEL STORM")
        self.assertEqual(expected, actual)

    def test__normalise_string_lower_case_input(self) -> None:
        expected = "Daniel Storm"
        actual = self.talent_csv_transform._normalise_string("daniel storm")
        self.assertEqual(expected, actual)

    def test__normalise_string_multiple_spaces(self) -> None:
        expected = "Daniel Storm"
        actual = self.talent_csv_transform._normalise_string("  Daniel   Storm ")
        self.assertEqual(expected, actual)

    def test__normalise_string_just_spaces(self) -> None:
        expected = ""
        actual = self.talent_csv_transform._normalise_string("  ")
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
