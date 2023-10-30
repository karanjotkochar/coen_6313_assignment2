import pymongo
import pandas as pd

# Connecting to MongoDB database on local machine
url = "mongodb://127.0.0.1:27017/"
client = pymongo.MongoClient(url)
Database = client['cloudassignment'] #Database name: cloudassignment

# Function to insert given 4 data set into database as 4 collections
def insert(collection, data):
    Database[collection].insert_many(data)

# To read data from CSV file
data = pd.read_csv("./data/" + 'DVD' + '-' + 'testing' + ".csv")
# data = pd.read_csv("./data/" + 'DVD' + '-' + 'training' + ".csv")
# data = pd.read_csv("./data/" + 'NDBench' + '-' + 'testing' + ".csv")
# data = pd.read_csv("./data/" + 'NDBench' + '-' + 'training' + ".csv")

# Creating records by adding one ID column at beginning and then inserting data as documents
data['_id']=range(0, len(data))

dvd_testing_data = data.to_dict(orient='record')
# dvd_training_data = data.to_dict(orient='record')
# ndbench_testing_data = data.to_dict(orient='record')
# ndbench_training_data = data.to_dict(orient='record')

insert("DVDTesting", dvd_testing_data)
# insert("DVDTraining", dvd_training_data)
# insert("NDBenchTesting", ndbench_testing_data)
# insert("NDBenchTraining", ndbench_training_data)

# Function to search one user from collection - for verification purpose
def search_one_user(collection, query):
    return Database[collection].find_one(query)

# For verification
# result = search_one_user("DVDTesting", {"_id":100})
# print(result)
# print(dvd_testing_data[100])