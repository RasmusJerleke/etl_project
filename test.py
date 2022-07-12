import unittest
from scripts import api_to_raw

class TestStringMethods(unittest.TestCase):

    def test_hej(self):
        self.assertEqual('hej', api_to_raw.hej())
        

if __name__ == '__main__':
    unittest.main()