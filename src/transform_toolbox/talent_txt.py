"""
Note:
    Use both content of the file and the name of each to generate one table:
        1. student name, date (the one in the filename), psychometrics, presentation
"""
import pandas as pd


class TalentTXT:
    def __init__(self) -> None:
        # self.raw_df = raw_df
        pass

    ##############################
    #   YOUR FUNCTIONS GO HERE   #
    ##############################

    def remove_word(self, df, column_name, word):
        ''' Removes a word, Use to remove Sparta Day and .text'''
        df[column_name] = df[column_name].str.replace(word,'')
        return df

    def transform_date(self, df, column_name):
        ''' Transforms the Date strings into DATETIME format
        i.e 1 August 2019 will be 2019-08-01'''
        df[column_name] = pd.to_datetime(df[column_name])
        for i in range(len(df[column_name])):
            df[column_name][i] = (df[column_name].iloc[i].day,df[column_name].iloc[i].month, df[column_name].iloc[i].year)
            df[column_name][i] = (list(df[column_name][i]))
        return df

    def rename_column(self, df, old_name, new_name):
        ''' Renames a column'''
        df = df.rename(columns = {old_name: new_name })
        return df

    def reorder_columns(self, df, new_columns: list):
        """
        :param df: dataframe
        :param new_columns: list of the new order of the columns
        :return: a dataframe with the new columns
        """
        df = df.reindex(columns = new_columns)
        return df

    def lower_case(self, df, column_name):
        df[column_name] = df[column_name].str.title()
        return df

    def del_duplicates(self,df):
        df = df.drop_duplicates()
        return df

    def change_dtypes(self,df, column_name, dtype):
        df = df.astype({column_name: dtype})
        return df

    def transform_talent_txt(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """
        Combines all previous functions to transform the file.
        """
        raw_df = self.del_duplicates(raw_df)
        raw_df = self.remove_word(raw_df, 'filename', 'Talent/Sparta Day')
        raw_df = self.remove_word(raw_df, 'filename', '.txt')
        raw_df = self.transform_date(raw_df, 'filename')
        raw_df = self.rename_column(raw_df, 'filename', 'date')
        raw_df = self.rename_column(raw_df, 'Name', 'student_name')
        raw_df = self.rename_column(raw_df, 'Psychometrics', 'psychometrics')
        raw_df = self.rename_column(raw_df, 'Presentation', 'presentation')
        raw_df = self.reorder_columns(raw_df, ['student_name', 'date', 'psychometrics', 'presentation'])
        raw_df = self.lower_case(raw_df,'student_name')
        raw_df = self.change_dtypes(raw_df, 'student_name', 'str')
        #################################
        #   YOUR OTHER CODE GOES HERE   #
        #################################
        return raw_df



if __name__ == '__main__':
    # run your code here for sanity checks
    df = pd.read_pickle('/Users/jamaomar/Documents/Data32/Final project v3/etl_pipeline_virtual_sparta_v2/app/pipeline/pickle_jar/talent_txt_v2.pkl')
    # df= Talent.remove_word(df, 'filename', 'Talent/Sparta Day')
    # df = Talent.remove_word(df, 'filename', '.txt')
    # df = Talent.transform_date(df,'filename')
    # df =Talent.rename_column(df, 'filename', 'Date')
    # df = Talent.reorder_columns(df, ['Name', 'Date', 'Psychometrics', 'Presentation'])
    # print(df)
    Talent = TalentTXT()
    print(type(Talent.transform_talent_txt(df)['date'][0]))
    pass
