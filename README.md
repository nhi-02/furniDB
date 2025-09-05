
## Requirements

- Python 3.10+

- MongoDB

- Python packages:

```
pip install pandas pymongo openpyxl
```
## Configuration

Edit the following variables in the script as needed:
```
EXCEL_PATH = "西尾家具製品一覧.xlsx"  # Path to your Excel file
DB_NAME    = "furniture_db"        # MongoDB database name
COL_FURN   = "Furniture"           # Furniture collection name
COL_TYPE   = "Type"                # Type collection name
COL_ROOM   = "Room"                # Room collection name
MONGO_URI  = "mongodb://localhost:27017"  # MongoDB URI
```
## Usage

1. Place the Excel file in the project folder (or cònig EXCEL_PATH).

2. Run script Python:
```
python furniture_inf.py
```

3. The script will:

- Delete old data in the Furniture collection.

- Create indexes for Type, Room, and Furniture.

- Import all rows from the Excel file into MongoDB.

- Report how many rows were imported and skipped.

- List any rows with missing `W` or `D` dimensions.
