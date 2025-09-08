from pymongo import MongoClient

def get_furniture_by_type(db, type_name):
    '''Get the list of Furniture by Room name (exact match)'''
    
    pipeline = [
        {
            "$lookup": {
                "from": "Type",
                "localField": "ID_type",
                "foreignField": "_id",
                "as": "type_info"
            }
        },
        {"$unwind": "$type_info"},
        {
            "$match": {
                "type_info.name": type_name
            }
        },
        {
            "$project": {
                "_id": 0,
                "code": 1,
                "W": 1,
                "D": 1,
                "H": 1,
                "type": "$type_info.name",
                "ID_room": 1
            }
        }
    ]
    return list(db["Furniture"].aggregate(pipeline))


def get_furniture_by_room(db, room_name):
    '''Get the list of Furniture by Room name (exact match)'''
    
    pipeline = [
        {
            "$lookup": {
                "from": "Room",
                "localField": "ID_room",
                "foreignField": "_id",
                "as": "room_info"
            }
        },
        {"$unwind": "$room_info"},
        {
            "$match": {
                "room_info.name": {"$regex": room_name, "$options": "i"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "code": 1,
                "W": 1,
                "D": 1,
                "H": 1,
                "room": "$room_info.name",
                "ID_type": 1
            }
        }
    ]
    return list(db["Furniture"].aggregate(pipeline))

def main():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["furniture_db"]  # đổi tên database

    # type_name = input("Input Type name: ").strip().strip('"')
    type_name = input("Input Type name:").strip().strip('"')
    room_name = input("Input Room name: ").strip().strip('"')

    furniture_by_type = get_furniture_by_type(db, type_name)
    furniture_by_room = get_furniture_by_room(db, room_name)

    print(f"\nFurniture by Type '{type_name}': {len(furniture_by_type)} item(s)")
    for f in furniture_by_type:
        print(f"Code: {f['code']}, W: {f['W']}, D: {f['D']}, H: {f['H']}")

    print(f"\nFurniture by Room '{room_name}': {len(furniture_by_room)} item(s)")
    for f in furniture_by_room:
        print(f"Code: {f['code']}, W: {f['W']}, D: {f['D']}, H: {f['H']}")

if __name__ == "__main__":
    main()
