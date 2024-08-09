PolRegioGTFS
============

Description
-----------

Creates GTFS file for the biggest Polish railway operator (by passenger numbers), [PolRegio](https://polregio.pl/).

Schedule data comes from their ticketing system API. It falls under Article 5.1 of the
[public sector data re-use act](https://isap.sejm.gov.pl/isap.nsf/DocDetails.xsp?id=WDU20160000352).
The agency has not expressed any requirements as described by Article 11 of that law (as of 2021-07-07),
thus, following Article 11.4, this data is available without any license-like requirements.

Stop positions are downloaded from my other project, [PLRailMap](https://github.com/MKuranowski/PLRailMap),
which is also released into the public domain (under CC0-1.0 license).

This means that the produced GTFS file can also be considered as if it was released into the public domain,
or under the CC0-1.0 license.


Running
-------

PolRegioGTFS is written in Python with the [Impuls framework](https://github.com/MKuranowski/Impuls).

To set up the project, run:

```terminal
$ python3 -m venv .venv
$ . .venv/bin/activate
$ pip install -Ur requirements.txt
```

Then, run:

```terminal
$ python3 -m polregio_gtfs
```

The resulting schedules will be put in a file called `polregio.zip`.
The scraper needs to make a lot of calls to the underlying API, so the process
might take a couple of minutes.

License
-------

_PolRegioGTFS_ is provided under the MIT license, included in the `LICENSE` file.
