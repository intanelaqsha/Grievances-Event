#STEP A - Expanded Sources
import pandas as pd
import ast

# Load file
df = pd.read_csv("Grievances-Grid view-1.csv", dtype=str)
df.columns = [c.strip() for c in df.columns]

# Tambahkan ID yang akan dipakai sampai Step 3
df["Raw_ID"] = df.index.astype(int)

# -----------------------------------------
# CLEAN + SPLIT function
# -----------------------------------------
def split_list(cell):
    if pd.isna(cell) or str(cell).strip() == "":
        return []
    s = str(cell).replace("[", "").replace("]", "")
    parts = [p.strip() for p in re.split("[,;]", s)]
    return list({p for p in parts if p})

# Columns to normalize
multi_cols = ["Suppliers", "Mills", "PIOConcessions", "Issues"]

for col in multi_cols:
    df[col] = df[col].apply(split_list)

# Source split (SPECIAL rule)
def split_source(val):
    if pd.isna(val) or str(val).strip() == "":
        return []
    s = str(val).replace("[", "").replace("]", "")
    parts = re.split("[,;]", s)
    return list({p.strip() for p in parts if p.strip()})

df["Source"] = df["Source"].apply(split_source)

# -----------------------------------------
# Step 1: Expand Source → 1 row per source
# -----------------------------------------
expanded_rows = []

for idx, row in df.iterrows():
    raw_id = row["Raw_ID"]
    sources = row["Source"]

    # Jika tidak ada source → tetap simpan 1 row
    if len(sources) == 0:
        new_row = row.copy()
        new_row["Source"] = None
        expanded_rows.append(new_row)
        continue

    # Jika ada banyak source → pecah
    for src in sources:
        new_row = row.copy()
        new_row["Source"] = src      # hanya 1 source
        new_row["Raw_ID"] = raw_id   # tetap sama
        expanded_rows.append(new_row)

df_expanded = pd.DataFrame(expanded_rows)

# Tambahkan Row_ID unik untuk Step 2–3
df_expanded["Row_ID"] = df_expanded.index.astype(int)

print("Original grievances:", df.shape[0])
print("Expanded rows:", df_expanded.shape[0])
df_expanded.head()
len(df_expanded)
