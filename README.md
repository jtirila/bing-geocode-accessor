# bing-geocode-accessor
PoC code to use Bing Spatial Services geocoding API using Python.

This will be very much just exploratory / PoC code to learn how to use the API,
nothing fancy or production ready.

I am aware of the [Geocoder Python package](https://pypi.python.org/pypi/geocoder).

However, it seems to me the package only supports geocoding one entry at a
time. I want to learn how to use the [Bing Geocoding Dataflow
API](https://msdn.microsoft.com/en-us/library/ff701733.aspx)  to create batch
geocoding jobs and retrieve the results.

## Status as of 2017-02-25

There is a somewhat working Python implementation. Parts of it were included in
this repository but as the code included hard-coded secrets etc, a chunk of
stuff was just removed and the file uploaded broken. So the current file is not
functional, will fix this soon.

To make it explicit that this is work in progress, the Python code was moved
into the initial-python branch. Feel free to investigate it there but keep in mind
that it is under construction.

