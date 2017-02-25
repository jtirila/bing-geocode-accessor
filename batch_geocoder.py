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


# TODO: * Some secrets have been removed from here and stuff needs to be migrated to e.g. environment variables.
# TODO:   So the code below is not functional as is, further work is needed.
# TODO: * Also, there are a lot of broken style conventions etc.
# TODO: * Also, some variables are poorly / misleadingly named, need to change those.

with open(TEST_CSV_RESPONSE_PATH) as csvfile:
    TEST_CSV_RESPONSE_DATA = csvfile.read()

with open(TEST_XML_RESPONSE_PATH) as xmlfile:
    TEST_XML_RESPONSE_DATA = xmlfile.read()


# Form the mock response objects for Mock processing
TestResponse = namedtuple('my_response', ['content'])
TEST_BING_RESPONSES = {'xml': TestResponse(TEST_XML_RESPONSE_DATA), 'csv': TestResponse(TEST_CSV_RESPONSE_DATA)}


# The individual response processors for XML and CSV
def _process_csv(request):
    return pandas.read_csv(StringIO(request.content), header=1)


def _process_xml(request):
    tree = ET.parse(StringIO(request.content))
    root = tree.getroot()
    df = pandas.DataFrame(map(lambda x: {'id': x.attrib['Id'], 'city': x[1][0].attrib['Locality'], 'lat': x[1][1].attrib['Latitude'], 'lng': x[1][1].attrib['Longitude']}, [elem for elem in root]))
    df.transpose()
    return df

# A lookup dict so that the previous processing functions can be called dynamically
type_process = {'xml': _process_xml, 'csv': _process_csv}


# Similar stuff for forming the requests
def _form_request_csv(df):
    csv = df.to_csv(sep=",", header=True, index=False)
    return "Bing Spatial Data Services, 2.0\n" + csv


def _form_request_xml(df):
    xml = ['<?xml version="1.0" encoding="utf-8"?>',
           '<GeocodeFeed xmlns = "http://schemas.microsoft.com/search/local/2010/5/geocode" Version = "2.0">']
    for row in df.iterrows():
        xml.append('  <GeocodeEntity Id="{}" xmlns="http://schemas.microsoft.com/search/local/2010/5/geocode">'.format(str(int(row[1]['Id']) + 1).zfill(3)))
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


def _read_example_data(type="csv"):
    with open(EXAMPLE_DATA_PATH, 'r') as csv_file:
        raw_data = pandas.read_csv(StringIO(csv_file.read()), delimiter=";", header=0)

        # TODO: maybe the unused ones can be dropped to remove bloat and just the relevant ones included
        # TODO: in the structure, need to check this out.
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
        df["GeocodeRequest/Address/PostalCode"] = raw_data['postcode'].apply(lambda x: str(x).zfill(5))
        df["GeocodeRequest/Culture"] = 'fi_FI'
        df["GeocodeRequest/Address/AddressLine"] = raw_data['streetAddress'].replace(numpy.nan, "", regex=True)
        df["GeocodeRequest/Address/Locality"] = raw_data['municipality'].replace(numpy.nan, "", regex=True)

        # For now, hard code country instead of reading it from source data.
        df["GeocodeRequest/Address/CountryRegion"] = 'Finland'

        # Modifications needed to make this Python 3.x compatible.
        df['Id'] = map(lambda x: str(x), range(len(df)))
        return type_form_request[type](df)


def _read_resource(r):
    json_resp = json.loads(r.text)
    return next(json_snippet['resources'][0] for json_snippet in json_resp['resourceSets'] if
                'resources' in json_snippet)


def _read_status(resource_json):
    return resource_json['status']


def _read_new_response(keyless_url):
    """Read a new iteration of the url. We expect that the key is not part of the url but rather we
    concatenate it to the url here."""
    return requests.get("{}?key={}".format(keyless_url, BING_API_KEY))


def _coordinate_overall_response(url, data, mock, type):
    """
    This creates the job and keeps looping to read results. The mock parameter is a development-time
    hack so that the bing service need not be hit every time if focusing on some other parts of the code

    Returns the response object.
    """
    if mock:
        return TEST_BING_RESPONSES[type]

    r = requests.post(url, data=data, headers={'content-type': 'text/plain, charset=UTF-8'})
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

    results_url = next(link['url'] for link in resource['links'] if (link['role'] == 'output' and
                                                                     link['name'] == "succeeded"))
    return _read_new_response(results_url)


def _persist_bing_response(r, type, write_bing_response):
    if not write_bing_response:
        return
    with open(os.path.join(THIS_FILE_DIR, 'test_data', "bing_example_response.{}".format(type)), "w") as outputfile:
        outputfile.write(r.content)


def create_job(type="csv", mock=False, write_bing_response=False):

    url = BING_API_URL_TEMPLATE.format(type)
    data = _read_example_data(type=type)

    r = _coordinate_overall_response(url, data, mock, type)
    _persist_bing_response(r, type, write_bing_response)

    return type_process[type](r)


def _read_saved_data():
    TEST_XML_RESPONSE_PATH


if __name__ == "__main__":
    data = create_job(type="csv", mock=False, write_bing_response=False)
    print data
