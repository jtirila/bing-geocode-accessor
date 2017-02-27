# bing-geocode-accessor
PoC code to use Bing Spatial Services geocoding API using Python.

This will be very much just exploratory / PoC code to learn how to use the API,
nothing fancy or production ready.

I am aware of the [Geocoder Python package](https://pypi.python.org/pypi/geocoder).

However, it seems to me the package only supports geocoding one entry at a
time. I want to learn how to use the [Bing Geocoding Dataflow
API](https://msdn.microsoft.com/en-us/library/ff701733.aspx)  to create batch
geocoding jobs and retrieve the results.

## Status as of 2017-02-27

There is now a somewhat working Python implementation. However, the code is still an
example script an not really a re-usable module. This will be fixed in subsequent
commits.

