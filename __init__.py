from pymongo import MongoClient

client = MongoClient('localhost:27017')
collection = client.kafka_db.topics

