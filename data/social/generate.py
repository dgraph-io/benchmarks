#!/usr/bin/python

from faker import Faker
import pandas as pd 
import gzip
import sys
from random import randint


faker = Faker()
def generate_data(size=500):
    # return a dataframe with user_name and phone number
    phones = [f'{faker.unique.msisdn()[4:]}' for i in range(size)]
    names = [faker.unique.user_name() for i in range(size)]
    df = pd.DataFrame({'user_name': names, 'phone_number': phones})
    return df


def dataframe_to_rdf(data, filehandle = sys.stdout):
    for _, row in data.iterrows():
        # add users and phone numbers to the rdf file
        rdf= ""
        rdf += "<_:{}> <phone_number> \"{}\" .\n".format(row['phone_number'],row['phone_number'])
        rdf += "<_:{}> <username> \"{}\" .\n".format(row['user_name'],row['user_name'])
        rdf += "<_:{}> <belongs_to> <_:{}> .\n".format(row['phone_number'],row['user_name'])
        # add follows relationship
        # get a random number of people to follow from the dataframe
        follows = data.sample(n=randint(5, 100))
        for _, row_target in follows.iterrows():
            if (row['user_name'] != row_target['user_name']):
                rdf += "<_:{}> <follows> <_:{}> .\n".format(row['user_name'],row_target['user_name'])
        # add contacts relationship
        contacts = data.sample(n=randint(5, 100))
        for _, row_target in contacts.iterrows():
            if (row['phone_number'] != row_target['phone_number']):
                rdf += "<_:{}> <has_in_contacts> <_:{}> .\n".format(row['phone_number'],row_target['phone_number'])
        filehandle.write(rdf)
    return


data = generate_data(10000)
# data.to_csv("products_with_embedding.csv.gz",index=False,compression='gzip',header=True)
# gzip file must use wt for write text
with gzip.open("./contacts.rdf.gz","wt") as f:
    dataframe_to_rdf(data, f)


# ## load data set
# Start the dgraph container with the following command
# docker run -it -d -p 8080:8080 -p 9080:9080 -v /path/to/dgraph-data:/dgraph --name dgraph-v24 dgraph/standalone:latest
# cp contacts.rdf.gz <local path to /dgraph-data>
# cp contacts.schema <local path to /dgraph-data>
# docker exec -it dgraph-v24 dgraph live -c 1 -f /dgraph/contacts.rdf.gz -s /dgraph/contacts.schema