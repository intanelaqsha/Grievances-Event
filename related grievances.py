import pandas as pd
import ast
import re

# Load file
df = pd.read_csv("Grievances-Grid view 3.csv", dtype=str)
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
    #s = str(val).replace("[", "").replace("]", "")
    s = str(val).strip()
    parts = re.split(r",(?!\s)", s)
    #parts = re.split("[,;]", s)
    parts = [p.strip() for p in parts if p.strip()]
    return parts

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


df2 = df_expanded.copy()

# Helper: unique sorted list
def uniq_list(x):
    return sorted(list(set(x)))

events = []
event_id = 1

# ---- CLUSTER PER SOURCE ----
for source, group in df2.groupby("Source"):
    group = group.reset_index(drop=True)

    source_events = []

    for idx, row in group.iterrows():

        # Ambil entitas
        sup = set(row["Suppliers"])
        mil = set(row["Mills"])
        pio = set(row["PIOConcessions"])
        iss = set(row["Issues"])

        # IMPORTANT → Grievance ID asli
        gid = row["ID"]

        # Date Filed asli
        date_filed = row["Date Filed"]

        merged = False

        # Cek overlap
        for evt in source_events:

            overlap = (
                len(sup & set(evt["Suppliers"])) > 0 or
                len(mil & set(evt["Mills"])) > 0 or
                len(pio & set(evt["PIOConcessions"])) > 0
            )

            if overlap:
                # Merge entitas
                evt["Suppliers"] = uniq_list(list(set(evt["Suppliers"]) | sup))
                evt["Mills"] = uniq_list(list(set(evt["Mills"]) | mil))
                evt["PIOConcessions"] = uniq_list(list(set(evt["PIOConcessions"]) | pio))
                evt["Issues"] = uniq_list(list(set(evt["Issues"]) | iss))

                # Merge grievance list
                evt["Grievance_List"].append(gid)
                evt["Grievance_List"] = uniq_list(evt["Grievance_List"])
                evt["Grievance_Count"] = len(evt["Grievance_List"])

                # Merge Date Filed → ambil yang paling lama
                evt["Date Filed_List"].append(date_filed)
                evt["Date Filed_List"] = uniq_list(evt["Date Filed_List"])
                evt["Date Filed"] = min(evt["Date Filed_List"])

                merged = True
                break

        # Tidak overlap → buat event baru
        if not merged:
            source_events.append({
                "Event_ID": f"EVT_{event_id}",
                "Source": source,

                "Suppliers": uniq_list(list(sup)),
                "Mills": uniq_list(list(mil)),
                "PIOConcessions": uniq_list(list(pio)),
                "Issues": uniq_list(list(iss)),

                "Grievance_List": [gid],
                "Grievance_Count": 1,

                # Simpan Date Filed list & final date
                "Date Filed_List": [date_filed],
                "Date Filed": date_filed
            })
            event_id += 1

    events.extend(source_events)

# Convert ke DataFrame
df_step2 = pd.DataFrame(events)

# Convert list → string
for col in ["Suppliers", "Mills", "PIOConcessions", "Issues", "Grievance_List", "Date Filed_List"]:
    df_step2[col] = df_step2[col].apply(lambda x: ", ".join(uniq_list(x)))

df_step2.to_csv("Step2.csv", index=False)
print('total', df_step2.shape[0])
df_step2.head(30)

# =========================================
# LOAD Step 2 (output dari Step 2)
# =========================================
df2 = pd.read_csv("Step2.csv", dtype=str)
df2.columns = [c.strip() for c in df2.columns]

# Convert list-like columns menjadi Python list
def to_list(cell):
    if pd.isna(cell) or cell.strip() == "":
        return []
    return [x.strip() for x in str(cell).split(",") if x.strip()]

list_cols = ["Suppliers", "Mills", "PIOConcessions", "Issues", "Grievance_List"]
for col in list_cols:
    df2[col] = df2[col].apply(to_list)


# =========================================
# LOAD file tambahan (Grievances-grid view 2.csv)
# berisi kolom Issues Combined
# =========================================
df_original_grievances = pd.read_csv("Grievances-Grid view 3.csv", dtype=str)
df_original_grievances.columns = [c.strip() for c in df_original_grievances.columns]

# Convert Issues Combined ke list
df_original_grievances["Issues Combined"] = df_original_grievances["Issues Combined"].apply(to_list)

# Create a mapping from original grievance ID to its 'Issues Combined'
id_to_issues_combined_map = df_original_grievances.set_index('ID')['Issues Combined'].to_dict()

# For each event in df2, collect all 'Issues Combined' from its constituent grievances
def get_event_issues_combined(grievance_list_ids):
    combined_issues = []
    for gid in grievance_list_ids:
        if gid in id_to_issues_combined_map:
            combined_issues.extend(id_to_issues_combined_map[gid])
    return sorted(list(set(combined_issues)))

df2['Issues Combined'] = df2['Grievance_List'].apply(get_event_issues_combined)

# Now, df2 contains the 'Issues Combined' column directly.
# Assign df2 to df to maintain the original variable name for the subsequent code.
df = df2


# =========================================
# STEP 3 – DEFINE ISSUE CATEGORY DICTIONARY
# =========================================
ISSUE_MAP = {
    "Environmental": [
        "Deforestation", "Peatland Loss", "Fires", "Riparian Issues",
        "Biodiversity loss", "Environmental Pollution"
    ],
    "Social": [
        "Labor Rights Violations", "Violence and/or Coercion",
        "Gender and Ethnic Disparities", "Human Rights Violation",
        "Labor Disputes", "Wage Dispute", "Forced Labor and/or Child Labor", "Limited Access to Services"
    ],
    "Land Conflict": [
        "Land Dispute", "Land Grabbing", "Indigenous Peoples Conflict"
    ],
    "Governance": [
        "Corruption", "Illegal Infrastructure", "Infrastructure Damage"
    ]
}

# Reverse map untuk lookup cepat
ISSUE_TO_GROUP = {}
for group, items in ISSUE_MAP.items():
    for it in items:
        ISSUE_TO_GROUP[it.lower()] = group


# =========================================
# STEP 3 – BUILDING GROUPED EVENTS
# =========================================
final_rows = []
new_eid = 1

for idx, row in df.iterrows():
    issues_raw = row["Issues Combined"] if isinstance(row["Issues Combined"], list) else []
    issues_raw = [x.strip() for x in issues_raw if x.strip()]

    # tampung issues berdasarkan kategori
    grouped = {}

    for issue in issues_raw:
        key = issue.lower()

        if key in ISSUE_TO_GROUP:
            cat = ISSUE_TO_GROUP[key]
        else:
            cat = "Other"

        if cat not in grouped:
            grouped[cat] = []
        grouped[cat].append(issue)

    # Untuk setiap kategori → buat event baru
    for cat, issue_list in grouped.items():
        new_row = {
            "Event_ID_S3": f"EVT3_{new_eid}",
            "Original_Event_ID": row["Event_ID"],
            "Issue_Category": cat,
            "Issues": ", ".join(sorted(set(issue_list))),
            "Suppliers": ", ".join(row["Suppliers"]),
            "Mills": ", ".join(row["Mills"]),
            "PIOConcessions": ", ".join(row["PIOConcessions"]),
            "Grievance_List": ", ".join(row["Grievance_List"]),
            "Grievance_Count": row["Grievance_Count"],
            "Source": row["Source"],
            "Date_Filed": row["Date Filed"]

        }
        final_rows.append(new_row)
        new_eid += 1

# Output akhir
df_step3 = pd.DataFrame(final_rows)
df_step3.to_csv("Step3.csv", index=False)

print("Step 3 selesai. Total events:", len(df_step3))
df_step3.head(40)

import pandas as pd
from datetime import datetime, timedelta

# =========================================
# LOAD Step 3
# =========================================
df3 = pd.read_csv("Step3.csv", dtype=str)
df3.columns = [c.strip() for c in df3.columns]

# Convert list-like ke Python list
def to_list(cell):
    if pd.isna(cell) or cell.strip() == "":
        return []
    return [x.strip() for x in str(cell).split(",") if x.strip()]

list_cols = ["Suppliers", "Mills", "PIOConcessions", "Grievance_List", "Source"]
for col in list_cols:
    df3[col] = df3[col].apply(to_list)

# Convert Date Filed ke datetime
df3["Date_Filed"] = pd.to_datetime(df3["Date_Filed"], errors='coerce')

# =========================================
# Step 4 – Merge logic baru
# =========================================
merged_events = []
mhid_id = 1

# ⏱ Time window rule
def in_time_window(issue_cat, date_new, date_latest):
    if pd.isna(date_new) or pd.isna(date_latest):
        return False

    delta_days = abs((date_new - date_latest).days)

    if issue_cat == "Environmental":
        return delta_days <= 90   # 3 bulan
    else:
        return delta_days <= 60   # 2 bulan

# Loop per Issue Category
for cat, group in df3.groupby("Issue_Category"):
    group = group.sort_values("Date_Filed").reset_index(drop=True)
    active_events = []

    for idx, row in group.iterrows():
        merged = False

        for evt in active_events:
            # Hitung overlap
            supplier_overlap = len(set(row["Suppliers"]) & set(evt["Suppliers"]))
            mill_overlap = len(set(row["Mills"]) & set(evt["Mills"]))
            plot_overlap = len(set(row["PIOConcessions"]) & set(evt["PIOConcessions"]))

            # ✅ Syarat baru:
            has_supplier = supplier_overlap >= 1
            has_asset = (mill_overlap >= 1 or plot_overlap >= 1)

            # Check merge
            if has_supplier and has_asset and in_time_window(cat, row["Date_Filed"], evt["Latest_Date"]):

                evt["Suppliers"] = sorted(list(set(evt["Suppliers"]) | set(row["Suppliers"])))
                evt["Mills"] = sorted(list(set(evt["Mills"]) | set(row["Mills"])))
                evt["PIOConcessions"] = sorted(list(set(evt["PIOConcessions"]) | set(row["PIOConcessions"])))
                evt["Source"] = sorted(list(set(evt["Source"]) | set(row["Source"])))
                evt["Grievance_List"] = sorted(list(set(evt["Grievance_List"]) | set(row["Grievance_List"])))
                evt["Grievance_Count"] = len(evt["Grievance_List"])

                # Update tanggal
                evt["Earliest_Date"] = min(evt["Earliest_Date"], row["Date_Filed"])
                evt["Latest_Date"] = max(evt["Latest_Date"], row["Date_Filed"])

                merged = True
                break

        # Jika tidak bisa di-merge → buat event baru
        if not merged:
            active_events.append({
                "MHID": f"MHID_{mhid_id}",
                "Issue_Category": cat,
                "Suppliers": row["Suppliers"],
                "Mills": row["Mills"],
                "PIOConcessions": row["PIOConcessions"],
                "Source": row["Source"],
                "Grievance_List": row["Grievance_List"],
                "Grievance_Count": row["Grievance_Count"],
                "Earliest_Date": row["Date_Filed"],
                "Latest_Date": row["Date_Filed"]
            })
            mhid_id += 1

    merged_events.extend(active_events)

# ========================================
# Convert ke DataFrame
# ========================================
df_step4 = pd.DataFrame(merged_events)

# Convert list → string
for col in ["Suppliers", "Mills", "PIOConcessions", "Source", "Grievance_List"]:
    df_step4[col] = df_step4[col].apply(lambda x: ", ".join(x))

# Convert tanggal ke string
df_step4["Earliest_Date"] = df_step4["Earliest_Date"].dt.strftime("%Y-%m-%d")
df_step4["Latest_Date"] = df_step4["Latest_Date"].dt.strftime("%Y-%m-%d")

df_step4.to_csv("Step4_MergedEvents_NewLogic.csv", index=False)

print("Step 4 selesai dengan logic baru ✅")
print("Total events:", len(df_step4))

df_step4.head(20)
