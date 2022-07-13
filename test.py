import unittest
from scripts import api_to_raw
from scripts import raw_to_harmonized


class TestAll(unittest.TestCase):

    def test_api_to_raw(self):
        # good request
        self.assertTrue(api_to_raw.get_data('https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16.158/lat/58.5812/data.json',
        'data/raw/data.json'))
        self.assertIsNone(api_to_raw.error_msg)

        # invalid request
        self.assertFalse(api_to_raw.get_data('httpssdaf://opendataasdsda-download-metadffdfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16.158/lat/58.5812/data.json',
        'data/raw/data.json'))
        self.assertEqual('failed to make request to url: httpssdaf://opendataasdsda-download-metadffdfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16.158/lat/58.5812/data.json', api_to_raw.error_msg)

        # invalid api args
        self.assertFalse(api_to_raw.get_data('https://opendata-download-metfcst.smhi.se/api/category/pmp3g11/version/2/geotype/point/lon/16.158/lat/58.5812/data.json',
        'data/raw/data.json'))
        self.assertEqual('status code not 200. Actual code = 404', api_to_raw.error_msg)

        # non existing folder
        self.assertFalse(api_to_raw.get_data('https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16.158/lat/58.5812/data.json',
        'data/bad_folder/data.json'))
        self.assertEqual('invalid path: data/bad_folder/data.json', api_to_raw.error_msg)

    def test_raw_to_harmonize(self):
        raw_to_harmonized.harmonized_data('data/raw/data.json')

        

if __name__ == '__main__':
    unittest.main()