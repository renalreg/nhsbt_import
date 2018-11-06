import unittest

from rr_ukt_import.dateutils import convert_datetime_string_to_datetime


class FormatDatetime(unittest.TestCase):

    def test_existing_date_formats_return_correctly(self):
        dates = ["01/02/12", "01-02-2012", "01FEB2012"]
        for d in dates:
            fd = convert_datetime_string_to_datetime(d)
            self.assertEqual(fd.year, 2012)
            self.assertEqual(fd.month, 2)
            self.assertEqual(fd.day, 1)

    def test_date_format_not_catered_for(self):
        d = "01FEB12"
        fd = convert_datetime_string_to_datetime(d)
        self.assertIsNone(fd)
