import re
from typing import Union, Type
import boto3
import pandas as pd
import json
from pathlib import Path


def txt_line_to_list(line: str) -> list[str]:
    """
    Convert a line of text into a list. Specifically designed to deal with the known text files in the bucket of
    interest.

    :param str line: line of text
    :return: list of elements in one row
    :rtype: list[str]
    """
    semi_row = re.split(r"\s*-\s*psychometrics", line, flags=re.I)
    # first few lines have less than three parts - they are describing the file, and we don't need them
    if len(semi_row) == 2:
        # first part is the name of the test participant
        name = semi_row[0].strip(" ,\r\n")
        # second part describes scores
        tests = semi_row[1].split(',')
        # split scores and remove names of each test
        psychometrics_val = tests[0].split(':')[1].strip(" ,\r\n")
        presentation_val = tests[1].split(':')[1].strip(" ,\r\n")
        # column names are hard-coded later to reduce complexity of the code
        return [name, psychometrics_val, presentation_val]


class ExtractFiles:
    """
    Example use:
    extract_files = ExtractFiles("data32-final-project-files")
    csv_df, extracted_csv_filenames = extract_files.get_files_as_df([], ext='.csv')
    json_df, extracted_json_filenames = extract_files.get_files_as_df([], ext='.json')
    txt_df, extracted_txt_filenames = extract_files.get_files_as_df([], ext='.txt')
    """

    def __init__(self, bucket_name: str) -> None:
        """
        Set up client and resource for S3 connection.

        :param str bucket_name: name of the S3 bucket
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.paginator = self.s3_client.get_paginator('list_objects_v2')

    def _get_json_file(self, key: str) -> pd.DataFrame:
        """
        Extracts a json file from the S3 bucket with given filename.

        :param str key: name of the file to extract
        :return: single-row dataframe with json file content
        :rtype: pd.DataFrame
        """
        s3_obj_body = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)["Body"]
        return pd.DataFrame([json.loads(s3_obj_body.read())])

    def _get_csv_file(self, key: str) -> pd.DataFrame:
        """
        Extracts a csv file from the S3 bucket with given filename.

        :param str key: name of the file to extract
        :return: single-row dataframe with csv file content
        :rtype: pd.DataFrame
        """
        return pd.read_csv(self.s3_client.get_object(Bucket=self.bucket_name, Key=key)["Body"])

    def _get_txt_file(self, key: str) -> pd.DataFrame:
        """
        Extracts a txt file from the S3 bucket with given filename and converts its content to appropriate row-like
        structure.

        :param str key: name of the file to extract
        :return: single-row dataframe with txt file content
        :rtype: pd.DataFrame
        """
        file_bytes = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)["Body"].readlines()
        # convert bytes to string and clean it using _txt_line_to_list
        file_list = []
        for i, line in enumerate(file_bytes):
            if i > 2:
                file_list.append(txt_line_to_list(line.decode("utf-8")))
        return pd.DataFrame(file_list, columns=["Name", "Psychometrics", "Presentation"])

    def _get_file(self, key: str) -> pd.DataFrame:
        """
        Extracts a file from the S3 bucket with given filename.

        :param str key: name of the file to extract
        :return: dataframe that contains data from the file
        :rtype: pd.DataFrame
        """
        # use different methods depending on the filetype
        if '.json' in key:
            return self._get_json_file(key)
        elif '.csv' in key:
            return self._get_csv_file(key)
        elif '.txt' in key:
            return self._get_txt_file(key)

    def _get_all_filenames_df(self, *, dtype: Union[Type[pd.DataFrame], Type[list]]) -> Union[pd.DataFrame, list[list[str]]]:
        """
        Returns all files in the bucket in one of the following formats:
            prefix      filename
        1   <prefix1>   <filename1>
        2   <prefix1>   <filename2>
        3   <prefix2>   <filename3>
        ...

        [
            [<prefix1>, <filename1>],
            [<prefix1>, <filename2>],
            [<prefix2>, <filename3>],
            ...
        ]

        :param Union[Type[pd.DataFrame], Type[list]] dtype: pd.DataFrame or list keywords
        :return: dataframe with all prefixes and filenames in the S3 bucket
        :rtype: pd.DataFrame
        """
        bucket_files = [obj.key for obj in self.s3_resource.Bucket(self.bucket_name).objects.all()]
        files_list = [[filename.split('/')[0], filename.split('/')[1]] for filename in bucket_files]
        if dtype == list:
            return files_list
        else:
            return pd.DataFrame(files_list, columns=["prefix", "filename"])

    def get_files_as_df(self, recorded_files: list[str], prefix: str, ext: str, *, filename_in_df: bool = False) -> Union[tuple[pd.DataFrame, pd.DataFrame], tuple[None, None]]:
        """
        Gather any files that are in S3 that were not recorded before.

        :param list[str] recorded_files: list of files that were recorded previously
        :param str ext: '.json' | '.csv' | '.txt'
        :param str prefix: 'Academy' | 'Talent'
        :param bool filename_in_df: if set to True the function will add an extra column with filename where the entry
        was taken from
        :return: tuple of list of dataframes containing all .json, .csv, .txt files and a dataframe with newly
        discovered files; it may return none if no new files present in the s3
        :rtype: Union[tuple[list[pd.DataFrame], pd.DataFrame], None]
        """
        filenames = self._get_all_filenames_df(dtype=list)

        files_content_list = []
        new_filenames = []

        no_of_recorded_files = len([f for f in recorded_files if ext in str(f) and prefix in str(f)])
        no_of_files = len([f for f in filenames if ext in str(f) and prefix in str(f)])
        print(f"Extracting {no_of_files - no_of_recorded_files} {ext} files from {self.bucket_name}/{prefix}...")

        for i, row in enumerate(filenames):
            fname = f"{row[0]}/{row[1]}"
            if fname not in recorded_files:
                if prefix in fname and ext in fname:
                    file_df = self._get_file(fname)
                    if filename_in_df:
                        file_df["filename"] = fname
                    files_content_list.append(file_df)
                    new_filenames.append([row[0], row[1]])

        if files_content_list:
            return (
                pd.concat(files_content_list, ignore_index=True),
                pd.DataFrame(new_filenames, columns=["prefix", "filename"])
            )
        else:
            return None, None


if __name__ == "__main__":
    # instantiate ExtractFiles class with the data32-final-project-files bucket
    extract_files = ExtractFiles("data32-final-project-files")

    # get names of all files in the bucket
    all_filenames = extract_files._get_all_filenames_df(dtype=list)

    pickle_jar_path = Path(__file__).parent.parent.resolve() / "pickle_jar"

    # save filenames in pickle file - sorted based on the path and filetype
    pd.DataFrame(
        [row for row in all_filenames if row[0] == "Academy" and ".csv" in row[1]], columns=["Prefix", "Filename"]
    ).to_pickle(pickle_jar_path / "academy_csv_filenames.pkl")
    pd.DataFrame(
        [row for row in all_filenames if row[0] == "Talent" and ".csv" in row[1]], columns=["Prefix", "Filename"]
    ).to_pickle(pickle_jar_path / "talent_csv_filenames.pkl")
    pd.DataFrame(
        [row for row in all_filenames if row[0] == "Talent" and ".json" in row[1]], columns=["Prefix", "Filename"]
    ).to_pickle(pickle_jar_path / "talent_json_filenames.pkl")
    pd.DataFrame(
        [row for row in all_filenames if row[0] == "Talent" and ".txt" in row[1]], columns=["Prefix", "Filename"]
    ).to_pickle(pickle_jar_path / "talent_txt_filenames.pkl")

    pd.set_option('display.max_columns', None)

    # extract content of files
    extract_files.get_files_as_df([], 'Academy', '.csv')[0].to_pickle(pickle_jar_path / "academy_csv.pkl")
    extract_files.get_files_as_df([], 'Talent', '.csv')[0].to_pickle(pickle_jar_path / "talent_csv.pkl")
    extract_files.get_files_as_df([], 'Talent', '.json')[0].to_pickle(pickle_jar_path / "talent_json.pkl")
    extract_files.get_files_as_df([], 'Talent', '.txt')[0].to_pickle(pickle_jar_path / "talent_txt.pkl")

    # extract content of files and include filenames in the table
    extract_files.get_files_as_df(
        [],
        'Academy',
        '.csv',
        filename_in_df=True
    )[0].to_pickle(pickle_jar_path / "academy_csv_v2.pkl")
    extract_files.get_files_as_df(
        [],
        'Talent',
        '.csv',
        filename_in_df=True
    )[0].to_pickle(pickle_jar_path / "talent_csv_v2.pkl")
    extract_files.get_files_as_df(
        [],
        'Talent',
        '.txt',
        filename_in_df=True
    )[0].to_pickle(pickle_jar_path / "talent_txt_v2.pkl")

    ################
    # Print tables #
    ################

    print(f"academy_csv.pkl:\n{pd.read_pickle(pickle_jar_path / 'academy_csv.pkl').columns}")
    print(f"talent_csv.pkl:\n{pd.read_pickle(pickle_jar_path / 'talent_csv.pkl').head()}")
    print(f"talent_json.pkl:\n{pd.read_pickle(pickle_jar_path / 'talent_json.pkl').columns}")
    print(f"talent_txt.pkl:\n{pd.read_pickle(pickle_jar_path / 'talent_txt.pkl').columns}")

    print(f"academy_csv_filenames.pkl:\n{pd.read_pickle(pickle_jar_path / 'academy_csv_filenames.pkl')}")
    print(f"talent_csv_filenames.pkl:\n{pd.read_pickle(pickle_jar_path / 'talent_csv_filenames.pkl')}")
    print(f"talent_json_filenames.pkl:\n{pd.read_pickle(pickle_jar_path / 'talent_json_filenames.pkl')}")
    print(f"talent_txt_filenames.pkl:\n{pd.read_pickle(pickle_jar_path / 'talent_txt_filenames.pkl')}")

    print(f"academy_csv.pkl:\n{pd.read_pickle(pickle_jar_path / 'academy_csv_v2.pkl').columns}")
    print(f"talent_csv.pkl:\n{pd.read_pickle(pickle_jar_path / 'talent_csv_v2.pkl').columns}")
    print(f"talent_txt.pkl:\n{pd.read_pickle(pickle_jar_path / 'talent_txt_v2.pkl').columns}")

