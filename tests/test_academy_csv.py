import unittest
from pathlib import Path
import pandas as pd
from app.pipeline.transform_toolbox.academy_csv import AcademyCSV


class TestAcademyCSV(unittest.TestCase):
    def setUp(self) -> None:
        self.pickle_jar_path = Path(__file__).parent.parent.resolve() / "pickle_jar"
        self.raw_df = pd.read_pickle(self.pickle_jar_path / "academy_csv_v2.pkl")
        self.filenames = pd.read_pickle(self.pickle_jar_path / "academy_csv_filenames.pkl")
        self.academy = AcademyCSV(self.raw_df)

    def test_get_dataframes_headers(self) -> None:
        """ Tests to see if the expected headers are the same as the extracted ones"""
        expected = ["Analytic", "Independent", "Determined", "Professional", "Studious", "Imaginative"]
        actual = self.academy.get_metrics_dataframe()
        self.assertEqual(expected, actual)

    def test_make_individual_dataframes(self) -> None:
        """ Tests to see how many individual dataframes are made(one per metric)"""
        expected = 6
        actual = len(self.academy.make_individual_dataframes())
        self.assertEqual(expected, actual)

    def test_return_not_null_rows(self) -> None:
        """ Tests to see if there are any null values in the dataframe"""
        for row in self.academy.dataframe_with_values().iterrows():
            self.assertIsNotNone(row)


    ##########################
    #   YOUR TESTS GO HERE   #
    ##########################

if __name__ == '__main__':
    unittest.main()
