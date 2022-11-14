import unittest
from pathlib import Path
import pandas as pd
from src.transform_toolbox.talent_txt import TalentTXT


class TestTalentTXT(unittest.TestCase):
    def setUp(self) -> None:
        self.TalentTXT = TalentTXT()
        self.pickle_jar_path = Path(__file__).resolve().parent.parent / "pickle_jar"
        self.raw_df = pd.read_pickle(self.pickle_jar_path / "talent_txt_v2.pkl")
        self.filenames = pd.read_pickle(self.pickle_jar_path / "talent_txt_filenames.pkl")

    def test_remove_word_column(self):
        df = pd.DataFrame({'words': ['Sparta Day Hi', 'Sparta Day Hello']})
        expected = pd.DataFrame({'words': [' Hi', ' Hello']})
        actual = self.TalentTXT.remove_word(df, 'words','Sparta Day')
        self.assertEqual(expected['words'][1],actual['words'][1])

    def test_transform_date(self):
        df = pd.DataFrame({'date': ['3 May 2015','2 March 2016']})
        expected_df = pd.DataFrame({'date': [[3,5,2015], [2,3,2016]]})
        actual = self.TalentTXT.transform_date(df, 'date')
        self.assertEqual(expected_df['date'][0], actual['date'][0])

    def test_rename_column(self):
        df = pd.DataFrame({'words': ['Sparta Day Hi', 'Sparta Day Hello'], 'year' : [2015,2016]})
        expected = pd.DataFrame({'stuff': ['Sparta Day Hi', 'Sparta Day Hello'], 'year' : [2015,2016]})
        actual = self.TalentTXT.rename_column(df, 'words', 'stuff')
        self.assertEqual(expected.columns[0],actual.columns[0])

    def test_reorder_column(self):
        df = pd.DataFrame({'words': ['Sparta Day Hi', 'Sparta Day Hello'], 'year' : [2015,2016]})
        expected = pd.DataFrame({'year':[2015,2016], 'words':['Sparta Day Hi', 'Sparta Day Hello']})
        actual = self.TalentTXT.reorder_columns(df, ['year','words'])
        self.assertEqual(expected.columns[0],actual.columns[0])

    def test_lower_case(self):
        df = pd.DataFrame({'words': ['SPARTA Day Hi', 'Sparta Day Hello'], 'year': [2015, 2016]})
        expected = pd.DataFrame({'words': ['Sparta Day Hi', 'Sparta Day Hello'], 'year': [2015, 2016]})
        actual = self.TalentTXT.lower_case(df, 'words')
        self.assertEqual(expected['words'][0],actual['words'][0])

    def test_del_duplicates(self):
        df = pd.DataFrame({'words': ['Sparta Day Hi','Sparta Day Hi', 'Sparta Day Hello'], 'year': [2015,2015, 2016]})
        expected = pd.DataFrame({'words': ['Sparta Day Hi', 'Sparta Day Hello'], 'year': [2015, 2016]})
        actual = self.TalentTXT.del_duplicates(df)
        self.assertEqual(len(expected.columns),len(actual.columns))


if __name__ == "__main__":
    unittest.main()
