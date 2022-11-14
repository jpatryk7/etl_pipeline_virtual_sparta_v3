"""
Note:
    Use both filenames and content of each file to generate three tables:
        1. trainer name
        2. course name, trainer name, date (the one from the filename)
        3. student name, date, course name, analytic, independent, etc.
"""
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class AcademyCSV:
    def __init__(self, raw_df) -> None:
        self.raw_df = raw_df

    def get_metrics_dataframe(self) -> list:

        """ The method returns the unique column names, which the students are assessed by """

        headers = [header.split("_")[0] for header in self.raw_df.columns if "_" in header]
        headers = list(dict.fromkeys(headers))

        return headers

    def courses_names_df(self) -> pd.DataFrame:

        """ Creates a dataframe that transforms the "filename" column into "course_name" and "date" """

        df = self.raw_df["filename"].copy()
        df = [a.split('/')[1] for a in df]
        list_of_courses = [x.split('.')[0] for x in df]
        course_list = [fname.rsplit("_", 1) for fname in list_of_courses ]
        course_df = pd.Series(list_of_courses).str.rsplit(r"_", n=1, expand=True)
        courses_df = pd.DataFrame(course_df)
        courses_df.columns = ["course_name", "date"]
        courses_df['course_name'] = courses_df['course_name'].str.replace('_', ' ')
        courses_df['date'] = pd.to_datetime(courses_df["date"])
        courses_df['date'] = courses_df['date'].dt.strftime('%d-%m-%Y')


        return courses_df

    def make_individual_dataframes(self) -> list:

        """ Returns a list of small dataframes, each corresponding to one metric, which students are assessed by"""

        headers_list = self.get_metrics_dataframe()
        course_df = self.courses_names_df()
        list_of_dfr = []
        for column_name in headers_list:
            small_df = self.raw_df[["name", f"{column_name}_W1", f"{column_name}_W2", f"{column_name}_W3",
                                    f"{column_name}_W4", f"{column_name}_W5", f"{column_name}_W6", f"{column_name}_W7",
                                    f"{column_name}_W8", f"{column_name}_W9", f"{column_name}_W10"]].copy()
            another_df = pd.concat([small_df, course_df], axis=1)
            another_df = pd.wide_to_long(another_df, stubnames=f"{column_name}", i=['name', "course_name", "date"],
                                         j='week', sep='_',
                                         suffix=r'\w+')
            another_df = another_df.reset_index(level=1, drop=False).reset_index().rename({'name': 'student_name'}, axis=1)
            list_of_dfr.append(another_df)

        return list_of_dfr

    def merge_dataframes(self):

        """ The method merges the smaller dataframes and drops duplicate columns   """

        list_of_dataframes = self.make_individual_dataframes()
        merged_dataframe = pd.concat(list_of_dataframes, axis=1)  # concatenates the dataframes within the list
        df = merged_dataframe.T.drop_duplicates().T  # gets rid of duplicate columns
        df = df.rename(columns=str.lower)

        return df.convert_dtypes()

    def dataframe_with_values(self) -> pd.DataFrame:

        """The method drops all the <NA> values from the previous dataframe"""

        merged_df = self.merge_dataframes()
        new_df = merged_df.dropna(axis=0)
        columnlist = list(new_df)
        for index, column in enumerate(columnlist):
            if index < 4:
                new_df = new_df.astype({column:object})
            else:
                new_df = new_df.astype({column: "int64"})
        columndate = new_df.iloc[:,1].tolist()
        datelist = []
        for element in columndate:
            datelist.append(element.split("-"))
        intdates = [[int(element) for element in dates] for dates in datelist]
        datesseries = pd.Series(intdates)
        new_df["date"] = datesseries.values
        return new_df
    def trainers_df(self) -> pd.DataFrame:

        """Returns a dataframe containing only the trainer names """

        df_trainers = self.raw_df[['trainer']].copy()
        df_trainers = df_trainers.drop_duplicates().rename({'trainer': 'trainer_name'}, axis=1)

        return df_trainers

    def course_trainers_df(self) -> pd.DataFrame:

        """The method combines the courses to their corresponding trainer"""

        course_df = self.courses_names_df()
        df_trainers = self.raw_df[['trainer']].copy()
        course_trainer_df = pd.concat([course_df, df_trainers], axis=1).drop_duplicates().rename({'trainer': 'trainer_name'}, axis=1)
        columndate = course_trainer_df.iloc[:,1].tolist()
        datelist = []
        for element in columndate:
            datelist.append(element.split("-"))
        intdates = [[int(element) for element in dates] for dates in datelist]
        datesseries = pd.Series(intdates)
        course_trainer_df["date"] = datesseries.values

        return course_trainer_df

    ##############################
    #   YOUR FUNCTIONS GO HERE   #
    ##############################

    def transform_academy_csv(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Passes the other methods into one function
        """
        headers = self.get_metrics_dataframe()
        dataframes = self.make_individual_dataframes()
        merged_df = self.merge_dataframes()
        academy_performance_df = self.dataframe_with_values()
        trainers_df = self.trainers_df()
        courses_df = self.course_trainers_df()

        return trainers_df, courses_df, academy_performance_df

if __name__ == "main":

    academy = AcademyCSV(pd.read_pickle('../../../../../Python/Projects/etl_pipeline_virtual_sparta_v3/pickle_jar/academy_csv_v2.pkl'))
    academy.transform_academy_csv()
#       print(academy.merge_dataframes().dtypes)
#     # print(academy.merge_dataframes())
#     # print(academy.trainers_df())
#     # print(academy.course_trainers_df())
#     # print(academy.dataframe_with_values())
#     # print(academy.make_individual_dataframes())
#     #print(academy.get_metrics_dataframe())
#     # print(pd.read_pickle('../pickle_jar/academy_csv_v2.pkl'))
#     # print(pd.read_pickle('../pickle_jar/academy_csv_filenames.pkl'))
