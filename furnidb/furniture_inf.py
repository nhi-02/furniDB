import os, re, math
import pandas as pd
from pymongo import MongoClient, ASCENDING, ReturnDocument

# ---- Config -----
EXCEL_PATH = "西尾家具製品一覧.xlsx"
DB_NAME    = "furniture_db"
COL_FURN   = "Furniture"
COL_TYPE   = "Type"
COL_ROOM   = "Room"
MONGO_URI  = os.getenv("MONGO_URI", "mongodb://localhost:27017") 

# ---- Text prepro ----

# Map full-width digits -> ASCII
FW_TO_ASCII = str.maketrans("０１２３４５６７８９．，－",
                            "0123456789.,-")

def normalize_text(s):
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s).strip() # nomarlize full-width to ASCII
    return s.translate(FW_TO_ASCII)

# Extract all numbers (int/float) from a string
NUM_RE = re.compile(r'[-+]?\d+\.?\d*')

def extract_all_numbers(text):
    text = normalize_text(text)
    return [m.group() for m in NUM_RE.finditer(text)]

def parse_value(val, mode="max"):
    """Return a number from an cell, handling both numeric and text values"""
    
    if pd.isna(val):
        return None

    # if its int (int/float) → return int
    if isinstance(val, (int, float)):
        return int(round(val))

    # is text → regex to find numbers
    text = normalize_text(val)
    nums = re.findall(r"[-+]?\d+\.?\d*", text)
    if not nums:
        return None
    nums = [float(n) for n in nums]
    return int(nums[0]) if mode == "first" else int(max(nums))

def flatten_headers(df: pd.DataFrame) -> pd.DataFrame:
    """If the header is a MultiIndex (due to merge), 
    flatten it into 'parent_child' and propagate parent values down to child columns (ffill)."""
    
    if isinstance(df.columns, pd.MultiIndex):
        # thay 'nan' bằng "" khi join
        df.columns = [
            "_".join([str(x) for x in tup if str(x) != "nan" and x is not None]).strip()
            for tup in df.columns.to_list()
        ]
    return df

def pick_column(df, candidates):
    """Pick column from candidates list, return None if not found"""
    
    for c in candidates:
        if c in df.columns:
            return c
    # thử tìm gần đúng (chứa)
    lc = [col for col in df.columns]
    for want in candidates:
        for col in lc:
            if want in str(col):
                return col
    return None

# ---- Load Excel ----

df_raw = pd.read_excel(EXCEL_PATH, header=[2, 3]) # header is row 2 and 3 (0-based)
df = flatten_headers(df_raw.copy())

# Column name candidates (because Japanese has full-width and half-width chars TvT)
COL_ROOM_CANDS = ["室名"]
COL_TYPENAME_CANDS = ["品名", "品   名"]
COL_CODE_CANDS = ["品番", "品 番"]

COL_W_CANDS = ["寸法_W", "Ｗ"]
COL_D_CANDS = ["寸法_D", "Ｄ"]
COL_H_CANDS = ["寸法_H", "Ｈ"]

col_room = pick_column(df, COL_ROOM_CANDS)
col_type = pick_column(df, COL_TYPENAME_CANDS)
col_code = pick_column(df, COL_CODE_CANDS)
col_w    = pick_column(df, COL_W_CANDS)
col_d    = pick_column(df, COL_D_CANDS)
col_h    = pick_column(df, COL_H_CANDS)

# Check code/mã sản phẩm trùng
'''print("Số dòng trong Excel:", len(df))
print("Số code unique trong Excel:", df[col_code].nunique())
print("Ví dụ 10 code:", df[col_code].head(10).tolist())

# Tìm các code bị trùng
dup_codes = df[col_code][df[col_code].duplicated(keep=False)]
dup_counts = dup_codes.value_counts()

print("Số code bị trùng:", len(dup_counts))
print("Top 10 code bị trùng:")
print(dup_counts.head(10))
'''
print(f"Picked columns:", {
    "Room": col_room,
    "Type": col_type,
    "Code": col_code,
    "W": col_w,
    "D": col_d,
    "H": col_h})

required = [col_room, col_type, col_code]
missing_req = [("Room", col_room), ("Type name", col_type), ("Code", col_code)]
missing_req = [name for name, val in missing_req if val is None]
if missing_req:
    raise RuntimeError(f"Missing required column: {', '.join(missing_req)}.\n"
                       f"Existing columns: {list(df.columns)}")

# ---- Connect Mongo ----
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
types = db[COL_TYPE]
rooms = db[COL_ROOM]
furns = db[COL_FURN]

furns.drop()   # Drop all existing furnitures because we will re-import all without uniqueness check

# Index/unique
types.create_index([("name", ASCENDING)], unique=True)
rooms.create_index([("name", ASCENDING)], unique=True)
furns.create_index([("code", ASCENDING)])

# ---- Upsert helpers ----
def upsert_type(name: str):
    name = normalize_text(name)
    doc = types.find_one_and_update(
        {"name": name},
        {"$setOnInsert": {"name": name}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return doc["_id"]

def upsert_room(name: str):
    name = normalize_text(name)
    doc = rooms.find_one_and_update(
        {"name": name},
        {"$setOnInsert": {"name": name}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return doc["_id"]

# ---- Main import ----
ok, skipped = 0, 0
errors = []

print("Importing furnitures...\n")

for i, row in df.iterrows():
    try:
        type_name = row[col_type]
        room_name = row[col_room]
        code      = row[col_code]

        type_id = upsert_type(type_name)
        room_id = upsert_room(room_name)

        W = parse_value(row[col_w], mode="max") if col_w else None
        D = parse_value(row[col_d], mode="max") if col_d else None
        H = parse_value(row[col_h], mode="max") if col_h else None
        
        # D is null -> W
        if D is None:
            D = W

        doc = {
            "row_index": int(i+1),
            "code": code,
            "W": W,
            "D": D,
            "H": H,
            "ID_type": type_id,
            "ID_room": room_id
        }

        furns.insert_one(doc)  # No uniqueness check, just insert
        ok += 1

    except Exception as e:
        skipped += 1
        errors.append((i, str(e)))

print(f"Done. OK: {ok}, Skipped: {skipped}")
if errors:
    print("Some errors (top 5):", errors[:5])

w_issues = list(furns.find({"W": None}, {"row_index": 1, "W":1}))
d_issues = list(furns.find({"D": None}, {"row_index": 1, "D":1}))

print("Excel W is null:", [doc["row_index"] for doc in w_issues])
print("Excel D is null:", [doc["row_index"] for doc in d_issues])
