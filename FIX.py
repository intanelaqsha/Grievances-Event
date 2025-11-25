import pandas as pd
import re
from datetime import datetime

# =====================================================
# CONFIG
# =====================================================
INPUT_FILE = "Grievances-Grid view 3.csv"
FINAL_OUT = "Final_Merged_NoIssueGrouping.csv"
TIME_WINDOW_DAYS = 90

# =====================================================
# HELPERS
# =====================================================
def split_list(cell):
    if pd.isna(cell) or str(cell).strip() == "":
        return []
    s = str(cell).replace("[", "").replace("]", "")
    parts = [p.strip() for p in re.split("[,;]", s)]
    return list({p for p in parts if p})

def split_source(val):
    if pd.isna(val) or str(val).strip() == "":
        return []
    s = str(val).strip()
    parts = re.split(r",(?!\s)", s)
    return [p.strip() for p in parts if p.strip()]

def to_list(cell):
    if pd.isna(cell) or str(cell).strip() == "":
        return []
    return [x.strip() for x in str(cell).split(",") if x.strip()]

def uniq_list(x):
    return sorted(list(set(x)))

def time_overlap(d1, d2, window_days):
    if pd.isna(d1) or pd.isna(d2):
        return False
    return abs((d1 - d2).days) <= window_days

# =====================================================
# STEP 1 – LOAD + NORMALIZE + EXPAND SOURCE
# =====================================================
print("\n[STEP 1] Load & Normalize...")

df = pd.read_csv(INPUT_FILE, dtype=str)
df.columns = [c.strip() for c in df.columns]
df["Raw_ID"] = df.index.astype(int)

multi_cols = ["Suppliers", "Mills", "PIOConcessions", "Issues"]

for col in multi_cols:
    df[col] = df[col].apply(split_list)

df["Source"] = df["Source"].apply(split_source)

expanded_rows = []

for _, row in df.iterrows():
    sources = row["Source"]

    if len(sources) == 0:
        new_row = row.copy()
        new_row["Source"] = None
        expanded_rows.append(new_row)
    else:
        for s in sources:
            new_row = row.copy()
            new_row["Source"] = s
            expanded_rows.append(new_row)

df_expanded = pd.DataFrame(expanded_rows)
df_expanded["Row_ID"] = df_expanded.index.astype(int)

print("✓ Total expanded rows:", len(df_expanded))

# =====================================================
# STEP 2 – MERGE PER SOURCE (NO EXPORT)
# =====================================================
print("\n[STEP 2] Clustering by Source...")

events = []
event_id = 1

for source, group in df_expanded.groupby("Source"):
    source_events = []

    for _, row in group.iterrows():
        sup = set(row["Suppliers"])
        mil = set(row["Mills"])
        pio = set(row["PIOConcessions"])
        iss = set(row["Issues"])

        gid = row["ID"]
        date_filed = row["Date Filed"]

        merged = False

        for evt in source_events:
            overlap = (
                len(sup & set(evt["Suppliers"])) > 0 or
                len(mil & set(evt["Mills"])) > 0 or
                len(pio & set(evt["PIOConcessions"])) > 0
            )

            if overlap:
                evt["Suppliers"] = uniq_list(evt["Suppliers"] + list(sup))
                evt["Mills"] = uniq_list(evt["Mills"] + list(mil))
                evt["PIOConcessions"] = uniq_list(evt["PIOConcessions"] + list(pio))
                evt["Issues"] = uniq_list(evt["Issues"] + list(iss))

                evt["Grievance_List"].append(gid)
                evt["Grievance_List"] = uniq_list(evt["Grievance_List"])
                evt["Grievance_Count"] = len(evt["Grievance_List"])

                evt["Date_Filed_List"].append(date_filed)
                evt["Date_Filed_List"] = uniq_list(evt["Date_Filed_List"])
                evt["Date_Filed"] = min(evt["Date_Filed_List"])

                merged = True
                break

        if not merged:
            source_events.append({
                "Event_ID": f"EVT_{event_id}",
                "Source": source,
                "Suppliers": list(sup),
                "Mills": list(mil),
                "PIOConcessions": list(pio),
                "Issues": list(iss),
                "Grievance_List": [gid],
                "Grievance_Count": 1,
                "Date_Filed_List": [date_filed],
                "Date_Filed": date_filed
            })
            event_id += 1

    events.extend(source_events)

df_step2 = pd.DataFrame(events)

print("✓ Total events after Step 2:", len(df_step2))

# =====================================================
# STEP 3 – FINAL MERGE (NO ISSUE GROUPING)
# =====================================================
print("\n[STEP 3] Final MHID merge...")

for col in ["Suppliers","Mills","PIOConcessions","Issues","Grievance_List","Source"]:
    df_step2[col] = df_step2[col].apply(lambda x: x if isinstance(x, list) else to_list(x))

df_step2["Date_Filed"] = pd.to_datetime(df_step2["Date_Filed"], errors="coerce")

merged_events = []
mhid = 1

for _, row in df_step2.iterrows():

    merged = False

    for evt in merged_events:

        supplier_overlap = len(set(row["Suppliers"]) & set(evt["Suppliers"])) >= 1
        infra_overlap = (
            len(set(row["Mills"]) & set(evt["Mills"])) >= 1 or
            len(set(row["PIOConcessions"]) & set(evt["PIOConcessions"])) >= 1
        )

        time_ok = time_overlap(row["Date_Filed"], evt["Latest_Date"], TIME_WINDOW_DAYS)

        if supplier_overlap and infra_overlap and time_ok:

            evt["Suppliers"] = uniq_list(evt["Suppliers"] + row["Suppliers"])
            evt["Mills"] = uniq_list(evt["Mills"] + row["Mills"])
            evt["PIOConcessions"] = uniq_list(evt["PIOConcessions"] + row["PIOConcessions"])
            evt["Issues"] = uniq_list(evt["Issues"] + row["Issues"])
            evt["Source"] = uniq_list(evt["Source"] + row["Source"])
            evt["Grievance_List"] = uniq_list(evt["Grievance_List"] + row["Grievance_List"])

            evt["Grievance_Count"] = len(evt["Grievance_List"])
            evt["Earliest_Date"] = min(evt["Earliest_Date"], row["Date_Filed"])
            evt["Latest_Date"] = max(evt["Latest_Date"], row["Date_Filed"])

            merged = True
            break

    if not merged:
        merged_events.append({
            "MHID": f"MHID_{mhid}",
            "Suppliers": row["Suppliers"],
            "Mills": row["Mills"],
            "PIOConcessions": row["PIOConcessions"],
            "Issues": row["Issues"],
            "Source": row["Source"],
            "Grievance_List": row["Grievance_List"],
            "Grievance_Count": len(row["Grievance_List"]),
            "Earliest_Date": row["Date_Filed"],
            "Latest_Date": row["Date_Filed"]
        })
        mhid += 1

df_final = pd.DataFrame(merged_events)

# =====================================================
# FINAL OUTPUT
# =====================================================
for col in ["Suppliers","Mills","PIOConcessions","Issues","Source","Grievance_List"]:
    df_final[col] = df_final[col].apply(lambda x: ", ".join(sorted(set(x))))

df_final["Earliest_Date"] = df_final["Earliest_Date"].dt.strftime("%Y-%m-%d")
df_final["Latest_Date"] = df_final["Latest_Date"].dt.strftime("%Y-%m-%d")

df_final.to_csv(FINAL_OUT, index=False)

print("\n✅ FINAL DONE (NO STEP 2 OUTPUT)")
print("Final MHID count:", len(df_final))
print("Output saved to:", FINAL_OUT)
print("Time window:", TIME_WINDOW_DAYS, "days")
