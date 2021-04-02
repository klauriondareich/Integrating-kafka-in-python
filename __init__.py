# This file is in case you want to save Producer messages to Mongo Db
from pymongo import MongoClient

client = MongoClient('localhost:27017')
collection = client.kafka_db.topics

