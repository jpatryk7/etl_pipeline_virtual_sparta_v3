import unittest
import pandas as pd


def date_format_test(date_ser: pd.Series) -> None:
    """
    Small helper function that checks if the given column contains only dates the format: [dd, mm, yyyy].
    :param pd.Series date_ser: pandas' column of lists
    """
    utest = unittest.TestCase()
    for i, entry in date_ser.items():
        utest.assertIsInstance(entry, list, f"Entry #{i} in {date_ser.name}, {entry}. Got type {type(entry)}")
        if entry:
            utest.assertEqual(len(entry), 3, f"Entry #{i} in {date_ser.name}, {entry}. Got length {len(entry)}")
            utest.assertEqual([type(x) for x in entry], [int, int, int], f"Entry #{i} in {date_ser.name}, {entry}. "
                                                                         f"Got types [{type(entry[0])}, "
                                                                         f"{type(entry[1])}, {type(entry[2])}].")
            utest.assertTrue(0 < entry[0] <= 31, f"Entry #{i} in {date_ser.name}, {entry}. Got {entry[0]} - not a "
                                                 f"proper day number.")
            utest.assertTrue(0 < entry[1] <= 12, f"Entry #{i} in {date_ser.name}, {entry}. Got {entry[1]} - not a "
                                                 f"proper month number")
            utest.assertTrue(1900 < entry[2] < 2022, f"Entry #{i} in {date_ser.name}, {entry}. Got {entry[2]} - not a "
                                                     f"proper year number.")
