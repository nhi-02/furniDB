import os
from pymongo import MongoClient

# ---- Config ----
DB_NAME = "furniture_db"
COL_TYPE = "Type"
COL_ROOM = "Room"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

def check_collection(col, col_name: str):
    print(f"\n Checking {col_name}.name for newline characters ...")
    bad_docs = list(col.find({"name": {"$regex": "\n"}}))

    if not bad_docs:
        print(f"No {col_name}.name contains newline (\\n)")
    else:
        for doc in bad_docs:
            # dùng repr() để nhìn thấy \n
            print(f"- _id={doc['_id']}, name={repr(doc['name'])}")
        print(f"Found {len(bad_docs)} documents with newline in '{col_name}.name'")

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    types = db[COL_TYPE]
    rooms = db[COL_ROOM]

    check_collection(types, "Type")
    check_collection(rooms, "Room")

if __name__ == "__main__":
    main()
