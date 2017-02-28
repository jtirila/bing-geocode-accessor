import enum
import pandas
import numpy
import json
import time
import os
import requests
from StringIO import StringIO


class GeocodingJob:

    # Define some class level types and constants internal to GeocodingJob:
    class GeocodingException(Exception):
        """GeocodingJob's custom error class"""
        pass

    class GCStatus(enum.Enum):
        """
        An enum class that defines the possible statuses in GeocodingJob
        """
        initialized = "initialized"
        job_created = "job_created"
        pending = "pending"
        bing_completed = "bing_completed"
        result_request_completed = "result_request_completed"
        completed = "completed"
        error = "error"

    BING_CSV_HEADERS = ["Id", "GeocodeRequest/Culture", "GeocodeRequest/Query",
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

    LOOP_WAIT_INTERVAL_SECONDS = 10

    def __init__(self, data, bing_key=None):
        """
        :param data: A pandas dataframe containing the columns 'id', 'streetAddress', 'municipality' and 'postcode'.
                     It is OK to have some missing values, but Bing *may* fail to geocode such entries.
        :param bing_key: A valid Bing spatial data API key. Can be omitted in which case an environment variable
               named BING_API_KEY is required. If both are nonexistent an exception is thrown.
        """
        bing_key = bing_key if bing_key is not None else os.environ["BING_API_KEY"]
        if bing_key is None:
            raise self.GeocodingException("You didn't provide a Bing API key. " +
                                          "Either provide the parameter or environment variable")
        self._request_payload = self._build_bing_request_payload(data)
        self._bing_key = bing_key
        self.status = self.GCStatus.initialized
        self._create_bing_job_url = "http://spatial.virtualearth.net/REST/v1/Dataflows/Geocode?input=csv" +\
                                   "&key={}".format(self._bing_key)

    # Public interface
    def fetch_results(self):
        """
        The public method that coordinates the process of fetching the results from Bing.
        :return: A dataframe containing all of the request and response columns
        """
        r = self._create_geocoding_job()
        response = self._loop_for_results(r)
        if not self.status == self.GCStatus.result_request_completed:
            self.status = self.GCStatus.error
            raise self.GeocodingException("Fetching the results failed somehow")
        df = self._process_csv_response(response)
        self.status = self.GCStatus.completed
        return df

    # Private methods
    def _create_geocoding_job(self):
        """
        Creates the geocoding job.

        :return: a requests module response object
        """

        response = requests.post(self._create_bing_job_url, data=self._request_payload,
                                 headers={'content-type': 'text/plain, charset=UTF-8'})
        self.status = self.GCStatus.job_created
        return response

    @staticmethod
    def _build_bing_request_payload(df):
        """
        Converts a dataframe with all the Bing-formatted input data into a properly formatted Bing CSV payload.

        :param df: A formatted dataframe containing the columns relevant for Bing CSV requests
        :return: A Bing request csv formed from the input df
        """
        # Modifications needed to make this Python 3.x compatible; also, the ID probaby needs to be a little bit
        # more unique and aso persisted somewhere.
        csv = GeocodingJob._build_payload_df(df).to_csv(sep=",", header=True, index=False)
        return "Bing Spatial Data Services, 2.0\n" + csv

    @staticmethod
    def _build_payload_df(raw_data):
        """
        From the raw input dataframe, build a version that can be used to construct Bing request CSV

        :param raw_data: The raw input data containing id, streetAddress, municipality and postcode for each record.
        :return: a dataframe formulated to contain Bing headers so that a Bing format payload can be formed from this
                 df by a simple pandas.to_csv
        """

        # TODO: maybe the unused ones can be dropped to remove bloat and just the relevant ones included
        # TODO: in the structure, need to check this out.
        # TODO: An initial observation is that the csv response will only include the fields listed in the request,
        # TODO:  which is kind of silly, no?
        df = pandas.DataFrame(columns=GeocodingJob.BING_CSV_HEADERS)
        df['Id'] = raw_data['id']
        df["GeocodeRequest/Address/PostalCode"] = raw_data['postcode'].apply(lambda x: str(x).zfill(5))
        df["GeocodeRequest/Culture"] = 'fi_FI'
        df["GeocodeRequest/Address/AddressLine"] = raw_data['streetAddress'].replace(numpy.nan, "", regex=True)
        df["GeocodeRequest/Address/Locality"] = raw_data['municipality'].replace(numpy.nan, "", regex=True)

        # For now, hard code country instead of reading it from source data.
        df["GeocodeRequest/Address/CountryRegion"] = 'Finland'
        return df

    @staticmethod
    def _read_resource(r):
        """
        :param r: A requests package response object, supposed to contain a bing status JSON
        :return: The first resource object contained in the JSON.
        """

        # TODO: it is a bit of a hack to just return the first one. No need for anything fancier at the moment though.
        json_resp = json.loads(r.content)
        return next(json_snippet['resources'][0] for json_snippet in json_resp['resourceSets'] if
                    'resources' in json_snippet)

    @staticmethod
    def _read_status(resource_json):
        """
        Just extracts the status string from the resource json.

        :param resource_json: A bing resource JSON
        :return: the string in the 'status' field
        """
        return resource_json['status']

    def _read_new_response(self, keyless_url):
        """
        Read a new iteration of the url. We expect that the key is not part of the url but rather we
        concatenate it to the url here. Returns a requests response object.

        :param keyless_url: A Bing url, read from the resource JSON
        :return: a requests module response object
        """
        return requests.get("{}?key={}".format(keyless_url, self._bing_key))

    def _loop_for_results(self, r):
        """
        Keeps waiting for the results for a particular geocoding job to be available.

        :param r: a requests module response object containing status information
        :return: a requests module response object containing actual geocoded data

        TODO: No error checking is performed so if something goes wrong, an uncaught exception will be thrown.
        TODO: This is currently bit of a hack and only suitable for situations where it is OK for the
        TODO: results waiting to block subsequent processing. It may be feasible to use some other async method
        TODO: in some scenarios. This will do for us for the time being, however.
        """

        resource = self._read_resource(r)
        status = self._read_status(resource)

        # TODO: check if this is robust enough... find the resources key and corresponding value (array), pick the
        # TODO: first element.

        print("Just about to enter the fetching loop")
        while status != 'Completed':
            if status == 'Pending':
                self.status == self.GCStatus.pending
            time.sleep(self.LOOP_WAIT_INTERVAL_SECONDS)
            resource = self._read_resource(self._read_new_response(resource['links'][0]['url']))
            status = self._read_status(resource)

        self.status = self.GCStatus.bing_completed
        response = self._read_new_response(next(link['url'] for link in resource['links'] if
                                           (link['role'] == 'output' and link['name'] == "succeeded")))
        self.status = self.GCStatus.result_request_completed
        return response

    @staticmethod
    def _process_csv_response(response):
        """
        Converts the CSV response from Bing into a dataframe

        :param response: a requests module response object containing the successful Bing CSV response payload
        :return: a dataframe formulated from the payload

        TODO: Maybe we don't need to return all of the payload... e.g. the request data could probably be dropped
        TODO: and just response included in the final dataframe.
        """
        return pandas.read_csv(StringIO(response.content), header=1)
