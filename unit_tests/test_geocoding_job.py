import unittest
import os
from StringIO import StringIO
import pandas
from test_data import TEST_DATA_DIR
from geocoding_job.geocoding_job import GeocodingJob
import requests_mock
import json
import re

STATUS_RESPONSE_CONTENT = json.dumps(
    {'resourceSets':
         [{'resources':
               [{'status': 'Completed', 'links':
                    [{'name': 'succeeded',
                      'role': 'output',
                      'url': 'http://spatial.virtualearth.net/foo/output/succeeded'
                      }
                     ]
                 }
                ]
           }
          ]
     })


with open(os.path.join(TEST_DATA_DIR,  'bing_example_response.csv')) as csvfile:
    TEST_BING_CSV_RESPONSE = csvfile.read()


class TestGeocodingData(unittest.TestCase):
    def setUp(self):
        with open(os.path.join(TEST_DATA_DIR, 'test_request_data.csv'), 'r') as testfile:
            self.test_data = pandas.read_csv(StringIO(testfile.read()), delimiter=";", header=0)

    def test_initialization(self):
        gc = GeocodingJob(self.test_data)
        self.assertTrue(isinstance(gc, GeocodingJob))

    @requests_mock.Mocker()
    def test_mock_data_works(self, mocker):
        matcher = re.compile("spatial.virtualearth.net")
        matcher_output = re.compile("output/succeeded")
        mocker.post(matcher, text=STATUS_RESPONSE_CONTENT)
        mocker.get(matcher_output, text=TEST_BING_CSV_RESPONSE)
        gc = GeocodingJob(self.test_data)
        results = gc.fetch_results()
        self.assertEqual(results[results["Id"] == 13]["GeocodeResponse/Address/Locality"].values[0], "Tampere")
        self.assertEqual(results[results["Id"] == 4]["GeocodeResponse/Address/Locality"].values[0], "Vantaa")
        self.assertEqual(results[results["Id"] == 7]["GeocodeResponse/Address/Locality"].values[0], "Helsinki")

    def test_live_data_fetched(self):
        gc = GeocodingJob(self.test_data)
        results = gc.fetch_results()
        # TODO: just a lightweight check that we got what was expected. Need to make more thorough?
        self.assertEqual(results[results["Id"] == 13]["GeocodeResponse/Address/Locality"].values[0], "Tampere")
        self.assertEqual(results[results["Id"] == 4]["GeocodeResponse/Address/Locality"].values[0], "Vantaa")
        self.assertEqual(results[results["Id"] == 7]["GeocodeResponse/Address/Locality"].values[0], "Helsinki")
