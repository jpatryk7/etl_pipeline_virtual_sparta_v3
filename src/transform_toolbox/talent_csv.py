import calendar
from typing import Union, Optional
import pandas as pd
import re
from pathlib import Path


class TalentCSV:
    """
    Example execution of the function:
        talent_csv = TalentCSV()
        info_df, inv_df = talent_csv.transform_talent_csv(df)
    """

    @staticmethod
    def __camel_case_reader(method: str, cc_list: Optional[list[str]] = None) -> Optional[list[str]]:  # pragma: no cover
        """
        Function that interfaces with a list of recorded so-far prefixes of camel case words. Possible modes of
        operation:
        1. no file found on read -> return empty list;
        2. no file found on write -> create file, write list to the file;
        3. file exists on read -> return it's content;
        4. file exists on write -> read it's content, concatenate with existing list and remove duplicates, then write;

        :param str method: can be set to 'read' or 'write' -> read/write data from the file record
        :param Optional[list[str]] cc_list: required if method set to 'write'; list of discovered prefixes
        :return: if method set to 'read' the function return list of recorded prefixes
        """
        filepath = Path(__file__).resolve().parent.parent / "camel_case_record" / "record.txt"
        if method == 'read':
            if filepath.is_file():
                # file exists on read -> return it's content
                with open(filepath, 'r') as file:
                    return file.readlines()
            else:
                # no file found on read -> return empty list;
                return []
        elif method == 'write':
            # ensure each prefix is normalised e.g. MAC -> Mac, van -> Van
            for i, pref in enumerate(cc_list):
                cc_list[i] = pref.lower().capitalize()

            if filepath.is_file():
                # file exists on write -> read it's content
                with open(filepath, 'r') as file:
                    old_record = file.readlines()
                # concatenate with existing list
                total_record = old_record + cc_list
                # remove duplicates, then write
                with open(filepath, 'w') as file:
                    file.writelines(set(total_record))
            else:
                # no file found on write -> create file, remove duplicates, write list to the file
                with open(filepath, 'w') as file:
                    file.writelines(set(cc_list))
        else:
            return None

    @staticmethod
    def _get_date_from_string(filename: str) -> list[int]:
        """
        Use the dataframe with filenames to extract dates and represent them in the format: [dd, mm, yyyy]

        :param str filename: filename the entry was taken from
        :return: dates
        """
        if not filename:
            return []

        months = {month: index for index, month in enumerate(calendar.month_abbr) if month}
        date_list = []
        for mo_abbr, mo_number in months.items():
            if mo_abbr.lower() in filename.lower():
                year_regex = re.compile(r"\d{4}")
                year = year_regex.search(filename)
                date_list = [1, mo_number, int(year.group())]
                break

        return date_list

    @staticmethod
    def _transform_dob(date: str) -> list[int]:
        """
        convert "dd/mm/yyyy" to [dd, mm, yyyy]

        :param str date: date as a string of the format dd/mm/yyyy
        :return: date as a list of integer numbers [dd, mm, yyyy]
        """
        if not date:
            return []
        else:
            return [int(x) for x in date.split('/')]

    def _transform_invited_date(self, day: Union[float, int], month_year: str) -> list[int]:
        """
        convert dd.0 and "mmmmmm yyyy" to [dd, mm, yyyy]

        :param Union[float, int] day: day of the month
        :param str month_year: moth and year e.g. "April 2019"
        :return: date in the format [dd, mm, yyyy]
        """
        if 'x' in month_year:
            # no month/year entry
            return []
        else:
            if day == 0.0:
                # no day entry
                day = 1

            date_ls = self._get_date_from_string(month_year)
            date_ls[0] = int(day)
            return date_ls

    @staticmethod
    def _normalise_string(s: str) -> str:
        """
        Makes sure each word in the string starts with capital letter. Takes into account camel cases.

        :param str s: input string e.g. 'MARTY MACCENZIE'
        :return: normalised string e.g. 'Marty MacCenzie' or 'Marty Maccenzie' - the latter may occur if the record is
        not populated with valid prefixes
        """
        # discard invalid strings
        if not s or s.isspace():
            return ''
        # remove duplicate spaces
        s = re.sub(r'\s+', ' ',   s).strip()
        # split into words and iterate
        s_list = []
        for word in s.split(' '):
            # special cases for some characters
            if word == '&':
                word = 'and'
            # allow only for letters and some special characters
            word = ''.join([c for c in word if c not in "0123456789!£$%^&*(),.@:;?/\\<>{}[]#~"])
            # lowercase all characters and capitalize first letter
            word = word.lower().capitalize()
            # special case for words with apostrophe at the beginning
            if word[0] == "'":
                word = word[0] + word[1].capitalize() + word[2:]
            s_list.append(word)
        # join words back together
        s = ' '.join(s_list)
        return s

    def transform_talent_csv(self, raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Clean up the data, change formatting and return collected student_information_df and invitation_df. The input
        shall have columns as such: 'name', 'gender', 'dob', 'email', 'city', 'address', 'postcode', 'phone_number',
        'uni', 'degree', 'invited_date', 'month', 'invited_by', 'filename'. The function deals with missing values in
        'dob', 'invited_date' and 'month' columns. It uses 'filename' to get a date and save in 'date' column. It also
        merges 'invited_date' and 'month' into 'invited_date'. All dates are converted to [dd, mm, yyyy] format.

        student_information_df.columns.tolist() >> [
                'student_name', 'date', 'gender', 'dob', 'email', 'city',
                'address','postcode', 'phone_number', 'uni', 'degree'
        ]
        invitation_df.columns.tolist() >> ['student_name', 'date', 'invited_date', 'invited_by']

        :param pd.DataFrame raw_df: dataframe with columns as listed above
        :return: student_information_df and invitation_df dataframes - columns listed above
        """
        # deal with nulls
        raw_df["dob"] = raw_df["dob"].fillna("")
        raw_df["invited_date"] = raw_df["invited_date"].fillna(0.0).astype(int).astype(str)
        raw_df["month"] = raw_df["month"].fillna("x")
        raw_df["invited_by"] = raw_df["invited_by"].fillna("")
        raw_df["city"] = raw_df["city"].fillna("")
        raw_df["uni"] = raw_df["uni"].fillna("")

        # merge day and month columns and apply transformations
        raw_df["invited_date"] = raw_df.apply(lambda x: self._transform_invited_date(x.invited_date, x.month), axis=1).copy()

        # change formatting of dates and student name
        raw_df["date"] = raw_df["filename"].apply(self._get_date_from_string)
        raw_df["dob"] = raw_df["dob"].apply(self._transform_dob)
        for key in ["name", "invited_by", "uni"]:
            raw_df[key] = raw_df[key].apply(self._normalise_string)

        # rename student name column
        raw_df.rename(columns={"name": "student_name"}, inplace=True)

        student_information_df = raw_df[["student_name", "date", "gender", "dob", "email", "city", "address", "postcode", "phone_number", "uni", "degree"]]
        invitation_df = raw_df[["student_name", "date", "invited_date", "invited_by"]]

        return student_information_df, invitation_df


if __name__ == '__main__':  # pragma: no cover
    pickle_jar_path = Path(__file__).resolve().parent.parent.parent / "pickle_jar"
    df = pd.read_pickle(pickle_jar_path / "talent_csv_v2.pkl")
    talent_csv = TalentCSV()
    info_df, inv_df = talent_csv.transform_talent_csv(df)

    pd.set_option('display.max_columns', None)
    print(info_df.head())
    print(inv_df.head())
