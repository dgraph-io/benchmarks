

# DonorsChoose data set
About [DonorsChoose.org](https://www.donorschoose.org/) :
Teachers come to DonorsChoose.org to request the materials and experiences they need most for their classrooms, and donors give to the projects that inspire them.

This data set is built from public data provided by DonorsChoose.org in a Kaggle project [datasets](https://www.kaggle.com/datasets/hanselhansel/donorschoose).

It has

- 59624 schools
- 376926 projects
- 1492841 donations
- 802332 donors


The logic is as follow
* Schools are located at a Zip code, city and state.
* Donors are located at a Zip code, city and state.
* A teacher launches a project for a school.
* The projects has a list of resources with qty and price.
* Donors give a certain amount for a given project.

Data has been pre-processed using a R script to clean the data set and add geolocation info to schools.

The dataset has been extended to include the geolocation of each zipcode using the [zipdataset](https://www.kaggle.com/datasets/joeleichter/us-zip-codes-with-lat-and-long) also found on Kaggle.

Random dates over 2 years have been added to the donations.

The CSV files have been processed to produce a list of triples and a schema has been manually created to set predicate indexes and entity types.

## How to load the dataset
Use Dgraph bulk loader or live loader.


## For beginners
You can rapidly start a dgraph instance using docker
> docker run --name mydgraph -d -p "8080:8080" -p "9080:9080"  -v ~/dgraph:/dgraph -v ~/out:/tmp -v ~/tmp:/data dgraph/standalone:latest

This command is using the local directory ""~/dgraph", use the local directory of your choice.
Copy the donors.rdf.gz and donors.schema files in this folder so they can be accessed in the docker image.

You can launch the dgraph live command :

>docker exec -it mydgraph  dgraph live -f /dgraph/donors.rdf.gz -s /dgraph/donors.schema

The data will be loaded in few seconds.

Then launch ratel (dgraph user interface):
>docker run --rm -d -p "8000:8000"  dgraph/ratel:latest

You are ready to experience Dgraph DQL on the donors data.
