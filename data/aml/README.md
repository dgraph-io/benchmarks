
IBM Transactions for Anti Money Laundering (AML)
from 
https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml

The folder contains a schema file and RDF files created from 
HI-Small_Trans.csv

RDF file has been created using [csv_to_rdf](https://github.com/hypermodeinc/dgraph-experimental/blob/main/data-import/csv-to-rdf/csv_to_rdf.py) python script from 
dgraph experimental repository, using the template file provided in this folder.

See the corresponding HI_Small_Patterns.tx file for generated fraudulent transactions. These transactions are labeled as Transaction.laundering true in the dataset.
 

