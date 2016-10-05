README
======

This folder contains sample geojson files that can be uploaded to Dgraph.

The countries, states and international airport data comes from Natural Earth
(NaturalEarthData.com). The zipcode data is from:
https://github.com/jgoodall/us-maps. US airport data is from GeoCommons.

## List of files
- **countries.geojson**: Country polygons from Natural Earth.
- **states_provinces.geojson**: State/province polygons from Natural Earth
- **intl_airports.geojson**: International aiport locations from Natural Earth
- **us_airports.geojson**: US airports locations from GeoCommons.
- **zcta5.json**: US Zip code polygons. Note: This file is very large.

## Uploading files to dgraph

To upload files geojson to dgraph you can use the `dgraphgoclient` as follows:

```
# Countries: Use ADM0_A3 as the unique key for countries.
$ dgraphgoclient --json countries.geojson --geoid ADM0_A3

# States/Provinces
$ dgraphgoclient --json states_provinces.geojson --geoid OBJECTID_1

# Intl airports
$ dgraphgoclient --json intl_airports.geojson --geoid abbrev

# US airports
$ dgraphgoclient --json us_airports.geojson

# Zip codes. Note this command will take a while to finish.
$ dgraphgoclient --json zcta5.json --geoid GEOID10

```


