from geocoding_job import GeocodingJob
import pandas
import os
from StringIO import StringIO

THIS_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data', 'test_request_data.csv')

with open(EXAMPLE_DATA_PATH, 'r') as csv_file:
    raw_data = pandas.read_csv(StringIO(csv_file.read()), delimiter=";", header=0)


gj = GeocodingJob(raw_data)

results = gj.fetch_results()

print results

