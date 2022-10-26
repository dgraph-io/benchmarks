

# DonorsChoose data set
About [DonorsChoose.org](https://www.donorschoose.org/) :
Teachers come to DonorsChoose.org to request the materials and experiences they need most for their classrooms, and donors give to the projects that inspire them.

This data set is built from public data provided by DonorsChoose.org in a Kaggle project [datasets](https://www.kaggle.com/datasets/hanselhansel/donorschoose).

It has
- 72083 schools
- 1274279 projects
- 899017 donations
- 2122640 donors
- 1077265 resources
- 402900 teachers

The logic is as follow
* Schools are located at a Zip code, city and state.
* Donors are located at a Zip code, city and state.
* A teacher launches a project for a school.
* The projects has a list of resources with qty and price.
* Donors give a certain amount for a given project.


The dataset has been extended to include the geolocation of each zipcode using the [zipdataset](https://www.kaggle.com/datasets/joeleichter/us-zip-codes-with-lat-and-long) also found on Kaggle.

Random dates over 2 years have been added to the donations.

The CSV files have been processed to produce a list of triples and a schema has been manually created to set predicate indexes and entity types.
