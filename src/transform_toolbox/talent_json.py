"""
Class helps to clean data. Input raw data, output is 4 tables with column names:
    1. student name, date, self-dev, geo-flex, financial support, result, course interest
    2. student name, date, strength
    3. student name, date, weakness
    4. student name, date, tech self score, tech score value
"""

import pandas as pd
import numpy as np
from typing import Tuple
from datetime import datetime


class TalentJSON:
    def __init__(self, raw_df: pd.DataFrame) -> None:
        self.dataframe = raw_df

    def remove_columns_for_weaknesses(self):
        """
        Create dataframe with name, date and weakness.
        :return: dataframe with name, date and weakness.
        :rtype: pd.DataFrame
        """
        new_dataframe = self.dataframe.drop(columns=['tech_self_score', 'strengths', 'self_development',
                                                     'geo_flex', 'financial_support_self', 'result', 'course_interest'])
        return new_dataframe

    def make_junction_weaknesses(self):
        """
        Splits weakness column to weakness_1, weakness_2, weakness_3.
        :return: dataframe with name, date and weakness_1, weakness_2, weakness_3.
        :rtype: pd.DataFrame
        """
        data = self.remove_columns_for_weaknesses()
        data[['weakness_1', 'weakness_2', 'weakness_3']] = pd.DataFrame(data.weaknesses.tolist(), index=data.index)
        data = data.drop(columns='weaknesses')
        return data

    def normalise_weaknesses(self):
        """
        Merge weaknesses to one column.
        :return: dataframe with columns: student_name, date, weakness.
        :rtype: pd.DataFrame
        """
        headers = ["weakness_1", "weakness_2", "weakness_3"]
        list_of_weaknesses = []
        for column_name in headers:
            small_df = self.make_junction_weaknesses()[['name', 'date', column_name]].copy()
            small_df = (pd.wide_to_long(small_df.reset_index(), stubnames="weakness", i=['index'], j='weaknesses',
                                        sep='_', suffix=r'\w+')).reset_index(level=[0, 1], drop=True)
            list_of_weaknesses.append(small_df)
        result = pd.concat(list_of_weaknesses)
        result = result.rename(columns={"name": "student_name"})



        result['date'] = pd.to_datetime(result['date'], dayfirst=True)
        result['day'] = result['date'].map(lambda x: x.day)
        result['month'] = result['date'].map(lambda x: x.month)
        result['year'] = result['date'].map(lambda x: x.year)
        result['date'] = result['day'].map(str) + '-' + result['month'].map(str) + '-' + result['year'].map(str)
        result['date'] = result.date.apply(lambda x: x.split('-'))
        result['date'] = result['date'].apply(lambda x: [int(i) for i in x])
        result = result.drop(['day', 'month', 'year'], axis=1)
        return result

    def remove_columns_for_strengths(self):
        """
        Create dataframe with name, date and strengths.
        :return: dataframe with name, date and strengths.
        :rtype: pd.DataFrame
        """
        new_dataframe = self.dataframe.drop(columns=['tech_self_score', 'weaknesses', 'self_development',
                                                     'geo_flex', 'financial_support_self', 'result', 'course_interest'])
        return new_dataframe

    def make_junction_strengths(self):
        """
        Splits weakness column to strength_1, strength_2, strength_3.
        :return: dataframe with name, date and strength_1, strength_2, strength_3.
        :rtype: pd.DataFrame
        """
        data = self.remove_columns_for_strengths()
        data[['strength_1', 'strength_2', 'strength_3']] = pd.DataFrame(data.strengths.tolist(), index=data.index)
        data = data.drop(columns='strengths')
        return data

    def normalise_strengths(self):
        """
        Merge weaknesses to one column.
        :return: dataframe with columns: student_name, date, strength.
        :rtype: pd.DataFrame
        """
        headers = ['strength_1', 'strength_2', 'strength_3']
        list_of_strengths = []
        for column_name in headers:
            small_df = self.make_junction_strengths()[['name', 'date', column_name]].copy()
            small_df = (pd.wide_to_long(small_df.reset_index(), stubnames="strength", i=['index'], j='strengths',
                                        sep='_', suffix=r'\w+')).reset_index(level=[0, 1], drop=True)
            list_of_strengths.append(small_df)
        result = pd.concat(list_of_strengths)
        result = result.rename(columns={"name": "student_name"})


        result['date'] = pd.to_datetime(result['date'], dayfirst=True)
        result['day'] = result['date'].map(lambda x: x.day)
        result['month'] = result['date'].map(lambda x: x.month)
        result['year'] = result['date'].map(lambda x: x.year)
        result['date'] = result['day'].map(str) + '-' + result['month'].map(str) + '-' + result['year'].map(str)
        result['date'] = result.date.apply(lambda x: x.split('-'))
        result['date'] = result['date'].apply(lambda x: [int(i) for i in x])
        result = result.drop(['day', 'month', 'year'], axis=1)
        return result

    def remove_columns_for_tech(self):
        """
        Create dataframe with name, date and strengths.
        :return: dataframe with name, date and strengths.
        :rtype: pd.DataFrame
        """
        new_dataframe = self.dataframe.drop(columns=['strengths', 'weaknesses', 'self_development',
                                                     'geo_flex', 'financial_support_self', 'result', 'course_interest'])
        return new_dataframe

    def make_junction_tech(self):
        """
        Splits weakness column to strength_1, strength_2, strength_3.
        :return: dataframe with name, date and strength_1, strength_2, strength_3.
        :rtype: pd.DataFrame
        """
        data = self.remove_columns_for_tech()
        self_score = pd.json_normalize(data['tech_self_score'])
        data = data.drop(columns='tech_self_score')
        data3 = pd.concat([data, self_score], axis=1)
        data3 = data3.rename(
            columns={'C#': 'tech_1', 'Java': 'tech_2', 'R': 'tech_3', 'JavaScript': 'tech_4', 'Python': 'tech_5',
                     'C++': 'tech_6', 'Ruby': 'tech_7', 'SPSS': 'tech_8', 'PHP': 'tech_9'})
        return data3

    def normalise_tech(self):
        """
        Merge weaknesses to one column.
        :return: dataframe with columns: student_name, date, strength.
        :rtype: pd.DataFrame
        """
        headers = ['tech_1', 'tech_2', 'tech_3', 'tech_4', 'tech_5', 'tech_6', 'tech_7', 'tech_8', 'tech_9']
        list_of_tech = []
        for column_name in headers:
            small_df = self.make_junction_tech()[['name', 'date', column_name]].copy()
            small_df = (pd.wide_to_long(small_df.reset_index(), stubnames='tech', i=['index'], j='tech_self_score',
                                        suffix=r'\w+')).reset_index(level=1, drop=False)
            list_of_tech.append(small_df)
        result = pd.concat(list_of_tech)
        result = result.rename(columns={"name": "student_name", 'tech': 'value'})
        result = result.replace(['_1', '_2', '_3', '_4', '_5', '_6', '_7', '_8', '_9'],
                                ['C#', 'Java', 'R', 'JavaScript', 'Python', 'C++', 'Ruby', 'SPSS', 'PHP'])
        result = result.dropna()
        result = result.astype({'value': np.int64})

        result['date'] = pd.to_datetime(result['date'], dayfirst=True)
        result['day'] = result['date'].map(lambda x: x.day)
        result['month'] = result['date'].map(lambda x: x.month)
        result['year'] = result['date'].map(lambda x: x.year)
        result['date'] = result['day'].map(str) + '-' + result['month'].map(str) + '-' + result['year'].map(str)
        result['date'] = result.date.apply(lambda x: x.split('-'))
        result['date'] = result['date'].apply(lambda x: [int(i) for i in x])
        result = result.drop(['day', 'month', 'year'], axis=1)
        return result

    def remove_columns(self) -> pd.DataFrame:
        """
        Remove columns tech_self_score, strengths, weaknesses from the main dataframe
        :return: dataframe with removed columns tech_self_score, strengths, weaknesses
        :rtype: pd.DataFrame
        """
        new_dataframe = self.dataframe.drop(columns=['tech_self_score', 'strengths', 'weaknesses'])
        new_dataframe = new_dataframe.rename(columns={"name": "student_name",
                                                      "financial_support_self": "financial_support"})
        new_dataframe['self_development'] = new_dataframe['self_development'].str.replace('No', '')
        new_dataframe['geo_flex'] = new_dataframe['geo_flex'].str.replace('No', '')
        new_dataframe['financial_support'] = new_dataframe['financial_support'].str.replace('No', '')
        new_dataframe['result'] = new_dataframe['result'].str.replace('Fail', '')
        new_dataframe = new_dataframe.astype({'geo_flex': bool, 'self_development': bool,
                                              'financial_support': bool, 'result': bool})

        new_dataframe['date'] = pd.to_datetime(new_dataframe['date'], dayfirst=True)
        new_dataframe['day'] = new_dataframe['date'].map(lambda x: x.day)
        new_dataframe['month'] = new_dataframe['date'].map(lambda x: x.month)
        new_dataframe['year'] = new_dataframe['date'].map(lambda x: x.year)
        new_dataframe['date'] = new_dataframe['day'].map(str) + '-' + new_dataframe['month'].map(str) + '-' + new_dataframe['year'].map(str)
        new_dataframe['date'] = new_dataframe.date.apply(lambda x: x.split('-'))
        new_dataframe['date'] = new_dataframe['date'].apply(lambda x: [int(i) for i in x])
        new_dataframe = new_dataframe.drop(['day', 'month', 'year'], axis=1)
        return new_dataframe

    def transform_talent_json(self) -> Tuple[
        pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Takes raw data and returns tuple with 4 tables:
        1. student name, date, self-dev, geo-flex, financial support, result, course interest
        2. student name, date, strength
        3. student name, date, weakness
        4. student name, date, tech self score, tech score value
        """
        emp = self.remove_columns()
        strengths = self.normalise_strengths()
        weakness = self.normalise_weaknesses()
        technic = self.normalise_tech()
        return emp, weakness, strengths, technic


from pathlib import Path

if __name__ == '__main__':
    pickle_jar_path = Path(__file__).parent.parent.resolve() / "pickle_jar"
    raw = pd.read_pickle(pickle_jar_path / 'talent_json.pkl')
    check = TalentJSON(raw)
    pass
