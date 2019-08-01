This repo contains code for trying to load 21M dataset into OrientDB. We try to convert the data
into JSON format and load it. We'll need one file for each entity e.g. Film, Director, Starring,
Genre, Country etc.

## Preparing JSON files

To prepare the JSON files, we need a list of ids for the entity and need to know what edges we need
to include in JSON. For e.g. the xids in `sorted_files.txt` were obtained from 21M rdf dataset and
all have a starring edge, hence are films.

These can be supplied to the `transform` binary to get the relevant JSON.

```
./transform -data ~/Downloads/21million.rdf.gz -properties initial_release_date,name -input sorted_films.txt -edges genre,starring,country
```

This would generate a `output.json` file for films (sample file in data/output.json). Similar files have to be generated for other
entities.

## Loading the data into OrientDB

[OrientDB ETL tool](http://www.orientdb.com/docs/last/Import-from-JSON.html) supports loading data
in JSON format. It can be run with a config file similar to config.json.

## Querying the data
