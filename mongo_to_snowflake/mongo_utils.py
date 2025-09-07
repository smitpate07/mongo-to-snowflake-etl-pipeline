import pymongo
import json
from mongo_to_snowflake.config import MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME

def fetch_from_mongo():
    client = pymongo.MongoClient(MONGO_URI)
    print("✅ Connected to MongoDB")
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    data = list(collection.find({}))
    for doc in data:
        if '_id' in doc:
            doc['_id'] = str(doc['_id'])
    count = len(data)

    print(f"📊 Extracted {count} documents from MongoDB")
    client.close()
    print("🔒 MongoDB connection closed.")

    return data, count
