# -*- coding: utf-8 -*-

import os
import requests
import pandas
import numpy
import time
import json
from collections import namedtuple
from StringIO import StringIO
import xml.etree.ElementTree as ET

# TODO: keeping this file for reference for now, however all real processing should be performed using the GeocodingJob
# TODO: class

LOOP_WAIT_INTERVAL_SECONDS = 10
BING_API_KEY = os.environ["BING_API_KEY"]
BING_API_URL_TEMPLATE = "http://spatial.virtualearth.net/REST/v1/Dataflows/Geocode?input={}" + "&key={}".format(BING_API_KEY)

THIS_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data', 'test_request_data.csv')

# TODO: the whole mock processing must be moved outside of this module, maybe include it in tests if relevant
# TODO: at all in the end
# Example responses from Bing, used for mocks
TEST_XML_RESPONSE_PATH = os.path.join(THIS_FILE_DIR, 'test_data', 'bing_example_response.xml')
TEST_CSV_RESPONSE_PATH = os.path.join(THIS_FILE_DIR, 'test_data', 'bing_example_response.csv')
with open(TEST_CSV_RESPONSE_PATH) as csvfile:
    TEST_CSV_RESPONSE_DATA = csvfile.read()

with open(TEST_XML_RESPONSE_PATH) as xmlfile:
    TEST_XML_RESPONSE_DATA = xmlfile.read()


# Form the mock response objects for Mock processing
TestResponse = namedtuple('my_response', ['content'])
# Mock the status payload
TEST_BING_INITIAL_RESPONSE = TestResponse(json.dumps({'resourceSets': [{'resources': [{'status': 'Completed'}]}]}))
# Mock the actual obtained response data payload
TEST_BING_DATA_RESPONSES = {'xml': TestResponse(TEST_XML_RESPONSE_DATA), 'csv': TestResponse(TEST_CSV_RESPONSE_DATA)}


# The individual response processors for XML and CSV
def _process_csv_response(request):
    return pandas.read_csv(StringIO(request.content), header=1)


def _process_xml_response(request):
    tree = ET.parse(StringIO(request.content))
    root = tree.getroot()
    df = pandas.DataFrame(map(lambda x: {'id': x.attrib['Id'],
                                         'city': x[1][0].attrib['Locality'],
                                         'lat': x[1][1].attrib['Latitude'],
                                         'lng': x[1][1].attrib['Longitude']},
                              [elem for elem in root]))
    df.transpose()
    return df

# A lookup dict so that the previous processing functions can be called dynamically
process_response_by_type = {'xml': _process_xml_response, 'csv': _process_csv_response}

# Similar stuff for forming the requests
def _form_request_csv(df):
    # Modifications needed to make this Python 3.x compatible; also, the ID probaby needs to be a little bit more unique
    # and aso persisted somewhere.
    csv = df.to_csv(sep=",", header=True, index=False)
    return "Bing Spatial Data Services, 2.0\n" + csv


def _form_request_xml(df):
    xml = ['<?xml version="1.0" encoding="utf-8"?>',
           '<GeocodeFeed xmlns = "http://schemas.microsoft.com/search/local/2010/5/geocode" Version = "2.0">']
    for row in df.iterrows():
        xml.append('  <GeocodeEntity Id="{}" xmlns="http://schemas.microsoft.com/search/local/2010/5/geocode">'.format(str(int(row[1]['Id'])).zfill(3)))
        xml.append('    <GeocodeRequest Culture="fi-FI" IncludeNeighborhood="0">')
        xml.append('      <Address AddressLine="{}" AdminDistrict="" Locality="{}" PostalCode="{}" />'.format(
            row[1]["GeocodeRequest/Address/AddressLine"],
            row[1]["GeocodeRequest/Address/Locality"],
            row[1]["GeocodeRequest/Address/PostalCode"]
        ))

        xml.append('    </GeocodeRequest>')
        xml.append('  </GeocodeEntity>')
    xml.append('</GeocodeFeed>')
    return '\n'.join(xml)

type_form_request = {'xml': _form_request_xml, 'csv': _form_request_csv}


def _read_example_data():
    with open(EXAMPLE_DATA_PATH, 'r') as csv_file:
        raw_data = pandas.read_csv(StringIO(csv_file.read()), delimiter=";", header=0)

    # TODO: maybe the unused ones can be dropped to remove bloat and just the relevant ones included
    # TODO: in the structure, need to check this out.
    # TODO: An initial observation is that the csv response will only include the fields listed in the request, which is
    # TODO: kind of silly, no?
    columns = ["Id", "GeocodeRequest/Culture", "GeocodeRequest/Query",
               "GeocodeRequest/Address/AddressLine", "GeocodeRequest/Address/AdminDistrict",
               "GeocodeRequest/Address/CountryRegion", "GeocodeRequest/Address/AdminDistrict2",
               "GeocodeRequest/Address/FormattedAddress", "GeocodeRequest/Address/Locality",
               "GeocodeRequest/Address/PostalCode", "GeocodeRequest/Address/PostalTown",
               "GeocodeRequest/ConfidenceFilter/MinimumConfidence",
               "ReverseGeocodeRequest/IncludeEntityTypes", "ReverseGeocodeRequest/Location/Latitude",
               "ReverseGeocodeRequest/Location/Longitude", "GeocodeResponse/Address/AddressLine",
               "GeocodeResponse/Address/AdminDistrict", "GeocodeResponse/Address/CountryRegion",
               "GeocodeResponse/Address/AdminDistrict2", "GeocodeResponse/Address/FormattedAddress",
               "GeocodeResponse/Address/Locality", "GeocodeResponse/Address/PostalCode",
               "GeocodeResponse/Address/PostalTown", "GeocodeResponse/Address/Neighborhood",
               "GeocodeResponse/Address/Landmark", "GeocodeResponse/Confidence", "GeocodeResponse/Name",
               "GeocodeResponse/EntityType", "GeocodeResponse/MatchCodes", "GeocodeResponse/Point/Latitude",
               "GeocodeResponse/Point/Longitude", "GeocodeResponse/BoundingBox/EastLongitude",
               "GeocodeResponse/BoundingBox/NorthLatitude", "GeocodeResponse/BoundingBox/WestLongitude",
               "GeocodeResponse/BoundingBox/SouthLatitude", "GeocodeResponse/QueryParseValues",
               "GeocodeResponse/GeocodePoints", "StatusCode", "FaultReason", "TraceId"]

    df = pandas.DataFrame(columns=columns)
    df['Id'] = raw_data['id']
    df["GeocodeRequest/Address/PostalCode"] = raw_data['postcode'].apply(lambda x: str(x).zfill(5))
    df["GeocodeRequest/Culture"] = 'fi_FI'
    df["GeocodeRequest/Address/AddressLine"] = raw_data['streetAddress'].replace(numpy.nan, "", regex=True)
    df["GeocodeRequest/Address/Locality"] = raw_data['municipality'].replace(numpy.nan, "", regex=True)

    # For now, hard code country instead of reading it from source data.
    df["GeocodeRequest/Address/CountryRegion"] = 'Finland'
    return df


def _read_resource(r):
    json_resp = json.loads(r.content)
    return next(json_snippet['resources'][0] for json_snippet in json_resp['resourceSets'] if
                'resources' in json_snippet)


def _read_status(resource_json):
    """Just extracts the status string from the resource json."""
    return resource_json['status']


def _read_new_response(keyless_url):
    """Read a new iteration of the url. We expect that the key is not part of the url but rather we
    concatenate it to the url here. Returns a requests response object."""
    return requests.get("{}?key={}".format(keyless_url, BING_API_KEY))


def _create_geocoding_job(url, request_payload, mock):
    """
    Inputs: the BING API url,
    This creates the job and keeps looping to read results. The mock parameter is a development-time
    hack so that the bing service need not be hit every time if focusing on some other parts of the code

    Returns the response object.
    """
    if mock:
        return TEST_BING_INITIAL_RESPONSE
    return requests.post(url, data=request_payload, headers={'content-type': 'text/plain, charset=UTF-8'})


def _loop_for_results(r, mock, payload_type):

    """Inputs:
     - r: a response object containing bing status payload.
     - mock: whether mocking or not (boolean)
     - type: either 'csv' or 'xml'

    Return value: a response object containing Bing result payload.

    TODO: no error checking is performed so if something goes wront, an uncaught exception will be thrown.

    TODO: this is currently bit of a hack and only suitable for situations where it is OK for the
    results waiting to block subsequent processing. It may be feasible to use some other async method
    in some scenarios. This will do for me, however."""

    resource = _read_resource(r)
    status = _read_status(resource)

    # TODO: check if this is robust enough... find the resources key and corresponding value (array), pick the
    # TODO: first element.

    print("Just about to enter the fetching loop")
    while status != 'Completed':
        print("Starting to wait and then fetch the resources")
        time.sleep(LOOP_WAIT_INTERVAL_SECONDS)
        resource = _read_resource(_read_new_response(resource['links'][0]['url']))
        status = _read_status(resource)

    if mock:
        return TEST_BING_DATA_RESPONSES[payload_type]
    return _read_new_response(next(link['url'] for link in resource['links'] if (link['role'] == 'output' and
                                                                         link['name'] == "succeeded")))


def _persist_bing_response(r, payload_type, write_bing_response):
    """
    This is just a development-time helper function to persist the responses from Bing for dev needs. Will be removed
    later on.

    Returns nothing, just writes the response to a file with a suitable extension if the write_bing_response
    parameter is set to true.
    """
    if not write_bing_response:
        return
    with open(os.path.join(THIS_FILE_DIR, 'test_data', "bing_example_response.{}".format(payload_type)), "w") as outputfile:
        outputfile.write(r.content)


def coordinate_example_job(payload_type="csv", mock=False, write_bing_response=False):
    """
    An example workflow to demonstrate how the current code can be used.

    :param payload_type: either 'csv' or 'xml'
    :param mock: Whether to just pretend to perform the Bing request and in reality just return previously persisted
                 values or not
    :param write_bing_response:  whether to persist bing response or not
    :return: a dataframe containing an example subset of the obtained values

    TODO: the returned subset is currently hard coded, needs to be made configurable
    """

    url = BING_API_URL_TEMPLATE.format(payload_type)
    data = _read_example_data()
    request_payload = type_form_request[payload_type](data)

    r = _create_geocoding_job(url, request_payload, mock)
    results_r = _loop_for_results(r, mock, payload_type)
    _persist_bing_response(results_r, payload_type, write_bing_response)
    return process_response_by_type[payload_type](results_r)


if __name__ == "__main__":
    type = "csv"
    data = coordinate_example_job(payload_type=type, mock=False, write_bing_response=True)
    print data
