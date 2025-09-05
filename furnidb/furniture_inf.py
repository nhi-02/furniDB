import os, re, math
import pandas as pd
from pymongo import MongoClient, ASCENDING, ReturnDocument

# ====== Config ======
EXCEL_PATH = "西尾家具製品一覧.xlsx"
DB_NAME    = "furniture_db"
COL_FURN   = "Furniture"
COL_TYPE   = "Type"
COL_ROOM   = "Room"
MONGO_URI  = os.getenv("MONGO_URI", "mongodb://localhost:27017")

# ====== Helpers ======

# Map full-width digits -> ASCII
FW_TO_ASCII = str.maketrans("０１２３４５６７８９．，－",
                            "0123456789.,-")

def normalize_text(s):
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s).strip()
    # chuẩn hoá full-width về ASCII
    return s.translate(FW_TO_ASCII)

# Lấy tất cả số (int/float) trong chuỗi
NUM_RE = re.compile(r'[-+]?\d+\.?\d*')

def extract_all_numbers(text):
    text = normalize_text(text)
    return [m.group() for m in NUM_RE.finditer(text)]

def parse_value(val, mode="max"):
    """Trả về số từ Excel cell, xử lý cả số và text"""
    if pd.isna(val):
        return None

    # Nếu là số (int/float) → trả thẳng về int
    if isinstance(val, (int, float)):
        return int(round(val))

    # Nếu là text → regex tách số
    text = normalize_text(val)
    nums = re.findall(r"[-+]?\d+\.?\d*", text)
    if not nums:
        return None
    nums = [float(n) for n in nums]
    return int(nums[0]) if mode == "first" else int(max(nums))

def flatten_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Nếu header là MultiIndex (do merge), flatten thành 'parent_child' và
    đẩy giá trị cha xuống các cột con (ffill)."""
    
    if isinstance(df.columns, pd.MultiIndex):
        # thay 'nan' bằng "" khi join
        df.columns = [
            "_".join([str(x) for x in tup if str(x) != "nan" and x is not None]).strip()
            for tup in df.columns.to_list()
        ]
    # lan truyền giá trị header bị trống do merge (hiếm khi cần sau flatten)
    # Không lan data, chỉ lan header bằng cách trên là đủ.
    return df

def pick_column(df, candidates):
    """Chọn cột phù hợp theo danh sách nhãn có thể (ưu tiên theo thứ tự)."""
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

# ====== Load Excel ======
# Không biết file m có bao nhiêu hàng header; thường header thật nằm ở hàng tiêu đề
# Nếu file có 2 hàng header (ví dụ: top '寸法' + dưới 'W D H'), dùng header=[*, *].
# Ở đây t để pandas tự đoán trước, sau đó flatten.
df_raw = pd.read_excel(EXCEL_PATH, header=[2, 3])
df = flatten_headers(df_raw.copy())

# Các tên cột ứng viên (tiếng Nhật/Anh) — chỉnh theo file thực tế nếu cần
COL_ROOM_CANDS = ["室名", "Room", "房名"]
COL_TYPENAME_CANDS = ["品名", "品   名", "商品名", "Type", "品　名"]
COL_CODE_CANDS = ["品番", "品 番", "Code", "型番"]

# Với W/D/H nằm dưới "寸法", sau khi flatten thường là: "寸法_W", "寸法_D", "寸法_H"
COL_W_CANDS = ["寸法_W", "Ｗ", "Width"]
COL_D_CANDS = ["寸法_D", "Ｄ", "Depth", "Length"]
COL_H_CANDS = ["寸法_H", "Ｈ", "Height"]

col_room = pick_column(df, COL_ROOM_CANDS)
col_type = pick_column(df, COL_TYPENAME_CANDS)
col_code = pick_column(df, COL_CODE_CANDS)
col_w    = pick_column(df, COL_W_CANDS)
col_d    = pick_column(df, COL_D_CANDS)
col_h    = pick_column(df, COL_H_CANDS)

# Check số unique sản phẩm
print("Số dòng trong Excel:", len(df))
print("Số code unique trong Excel:", df[col_code].nunique())
print("Ví dụ 10 code:", df[col_code].head(10).tolist())

# Tìm các code bị trùng
dup_codes = df[col_code][df[col_code].duplicated(keep=False)]
dup_counts = dup_codes.value_counts()

print("Số code bị trùng:", len(dup_counts))
print("Top 10 code bị trùng:")
print(dup_counts.head(10))

# Check lỗi null ở các cột kích thước
print("Picked columns:")
print("Room:", col_room)
print("Type:", col_type)
print("Code:", col_code)
print("W:", col_w)
print("D:", col_d)
print("H:", col_h)

# In thử vài giá trị của các cột số để xem thực chất Excel đọc ra gì
print("\nSample W values:", df[col_w].dropna().head(10).tolist() if col_w else "Không tìm thấy cột W")
print("Sample D values:", df[col_d].dropna().head(10).tolist() if col_d else "Không tìm thấy cột D")
print("Sample H values:", df[col_h].dropna().head(10).tolist() if col_h else "Không tìm thấy cột H")



required = [col_room, col_type, col_code]
missing_req = [("Room", col_room), ("Type name", col_type), ("Code", col_code)]
missing_req = [name for name, val in missing_req if val is None]
if missing_req:
    raise RuntimeError(f"Thiếu cột bắt buộc: {', '.join(missing_req)}.\n"
                       f"Các cột hiện có: {list(df.columns)}")

# ====== Connect Mongo ======
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
types = db[COL_TYPE]
rooms = db[COL_ROOM]
furns = db[COL_FURN]

furns.drop()   # Xóa toàn bộ dữ liệu cũ

# Index/unique
types.create_index([("name", ASCENDING)], unique=True)
rooms.create_index([("name", ASCENDING)], unique=True)
furns.drop_index("code_1")
furns.create_index([("code", ASCENDING)])

# ====== Upsert helpers ======
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

# ====== Main import ======
ok, skipped = 0, 0
errors = []

for i, row in df.iterrows():
    try:
        type_name = row[col_type]
        room_name = row[col_room]
        code      = normalize_text(row[col_code])

        # bỏ kiểm tra duplicate code nếu muốn insert tất cả
        # if not code:
        #     skipped += 1
        #     continue

        type_id = upsert_type(type_name)
        room_id = upsert_room(room_name)

        W = parse_value(row[col_w], mode="max") if col_w else None
        D = parse_value(row[col_d], mode="max") if col_d else None
        H = parse_value(row[col_h], mode="max") if col_h else None

        doc = {
            "code": code,
            "W": W,
            "D": D,
            "H": H,
            "ID_type": type_id,
            "ID_room": room_id
        }

        furns.insert_one(doc)  # insert trực tiếp
        ok += 1

    except Exception as e:
        skipped += 1
        errors.append((i, str(e)))

print(f"Done. OK: {ok}, Skipped: {skipped}")
if errors:
    print("Some errors (top 5):", errors[:5])

