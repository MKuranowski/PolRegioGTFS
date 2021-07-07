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

PolRegioGTFS is written in TypeScript for the [Deno runtime](https://deno.land/).
Additionally, in order to create a zip file, Info-Zip is required - on most Linux distribution
the required package is named `zip`.

After installing the following requirements run `./polregiogtfs.sh` - which sets up
required permissions for the Deno runtime and simply calls PolRegioGTFS.main().

As it is necessary to make a lot of calls to the API in order to get _all_ of the schedules
it might take a dozen or so minutes for the script to complete.

License
-------

_PolRegioGTFS_ is provided under the MIT license, included in the `LICENSE` file.
