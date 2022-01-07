import certifi

ca = certifi.where()
import pymongo

client = pymongo.MongoClient(
    "mongodb+srv://dmodzelewski:Start1234@hurtowniedanych.oqbfe.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",
    tlsCAFile=ca)

db = client.test
db.coll.insert_one({"Test": "Testtt"})
