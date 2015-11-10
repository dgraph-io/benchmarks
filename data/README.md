README
======

The data is derived from Freebase Data Dumps:
Google, Freebase Data Dumps, https://developers.google.com/freebase/data, Nov 10, 2015.

**film-entities.txt.gz** contains film related entities, one entity per line (not RDF). Contains 4.6 million entities.
**names.gz** contains `type.object.name` RDF entries for film related entities. Contains 4.1 million names (edges) in various languages.
**rdf-films.gz** contains the RDF data for all the entities which have a name entry in `names.gz`. Currently contains ~17 million edges.
**langnames.gz** contains names of primary languages.
```
zcat frdf-films.gz | wc -l
16972467
```

Film data from Freebase is like so:
film.film --{film.film.starring}--> [mediator] --{film.performance.actor}--> film.actor

```
# Film --> Mediator
$ zgrep "<film.film.starring>" rdf-films.gz | wc -l
1397647

# TODO: The following is an artifact of how data was filtered from freebase. Fix this.
# Mediator --> Actor
$ zgrep "<film.performance.actor>" rdf-films.gz | wc -l
227

# Film --> Director
$ zgrep "<film.film.directed_by>" rdf-films.gz | wc -l
242212

# Director --> Film
$ zgrep "<film.director.film>" rdf-films.gz | wc -l
245274

# Film --> Initial Release Date
$ zgrep "<film.film.initial_release_date>" rdf-films.gz | wc -l
240858

# TODO: Generate Year of Release --> Film

# Film --> Genre
$ zgrep "<film.film.genre>" rdf-films.gz | wc -l
548152

# Genre --> Film
$ zgrep "<film.film_genre.films_in_this_genre>" rdf-films.gz | wc -l
546698

# TODO: film.film.runtime points to a mediator node, but then leads to nowhere. Remove this data.

# Film --> Primary Language
$ zgrep "<film.film.primary_language>" rdf-films.gz | awk '{print $3}' | uniq | sort | uniq | wc -l
55

# TODO: Generate Primary Language --> Film.

# Generated language names from names freebase rdf data.
$ zcat langnames.gz | awk '{print $1}' | uniq | sort | uniq | wc -l
55

# Film --> Country
$ zgrep "<film.film.country>" rdf-films.gz | wc -l
255239

# Total number of countries.
$ zgrep "<film.film.country>" rdf-films.gz | awk '{print $3}' | uniq | sort | uniq | wc -l
304

# Generated country names from names freebase rdf data.
$ zcat countrynames.gz | awk '{print $1}' | sort | uniq | wc -l
304
```
