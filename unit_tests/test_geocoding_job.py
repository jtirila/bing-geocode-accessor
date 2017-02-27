import unittest
import os
from StringIO import StringIO
import pandas
from test_data import TEST_DATA_DIR
from geocoding_job.geocoding_job import GeocodingJob

# TODO: it is probably good to include some kind of mocking also to test functionality outside of the actual Bing
# TODO: API request.


class TestGeocodingData(unittest.TestCase):
    def setUp(self):
        with open(os.path.join(TEST_DATA_DIR, 'test_request_data.csv'), 'r') as testfile:
            self.test_data = pandas.read_csv(StringIO(testfile.read()), delimiter=";", header=0)

    def test_initialization(self):
        gc = GeocodingJob(self.test_data)
        self.assertTrue(isinstance(gc, GeocodingJob))

    def test_mock_data_works(self):
        pass

    def test_live_data_fetched(self):
        gc = GeocodingJob(self.test_data)
        results = gc.fetch_results()
        self.assertEqual(results[results["Id"] == 13]["GeocodeResponse/Address/Locality"].values[0], "Tampere")
        self.assertEqual(results[results["Id"] == 4]["GeocodeResponse/Address/Locality"].values[0], "Vantaa")
        self.assertEqual(results[results["Id"] == 7]["GeocodeResponse/Address/Locality"].values[0], "Helsinki")
