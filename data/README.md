README
======

The data is derived from Freebase Data Dumps:
Google, Freebase Data Dumps, https://developers.google.com/freebase/data, Nov 10, 2015.

**film-entities.txt.gz** contains film related entities, one entity per line (not RDF). Contains 4.6 million entities.
**names.gz** contains `type.object.name` RDF entries for film related entities. Contains 4.1 million names (edges) in various languages.
**rdf-films.gz** contains the RDF data for all the entities which have a name entry in `names.gz`. Currently contains ~17 million edges.
```
zcat frdf-films.gz | wc -l
16972467
```
