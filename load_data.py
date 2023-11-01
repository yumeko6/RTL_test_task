from os import getenv

import bson
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient


load_dotenv()

# Create a MongoDB client and get a reference to the collection
client = AsyncIOMotorClient(
        f"mongodb://{getenv('HOST')}:{getenv('PORT')}"
    )
collection = client[getenv("DATABASE")][getenv("COLLECTION")]

# Bulk insert provided data
with open("sample_collection.bson", "rb") as bson_file:
    collection.insert_many(bson.decode_all(bson_file.read()))
