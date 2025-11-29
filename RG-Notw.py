import pandas as pd
import re
from datetime import datetime

# =====================================================
# CONFIG
# =====================================================
INPUT_FILE = "Grievances-Grid view 3.csv"
FINAL_OUT = "Final_Merged_Notw.csv"

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
    parts = re.split(r",(?=\S)", s)
    return [p.strip() for p in parts if p.strip()]

def to_list(cell):
    if pd.isna(cell) or str(cell).strip() == "":
        return []
    return [x.strip() for x in str(cell).split(",") if x.strip()]

def uniq_list(x):
    return sorted(list(set(x)))

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
# STEP 2 – MERGE PER SOURCE (same as your version)
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
# STEP 3 – FINAL MERGE (NO TIME WINDOW)
# =====================================================
print("\n[STEP 3] Final MHID merge...")

for col in ["Suppliers","Mills","PIOConcessions","Issues","Grievance_List","Source"]:
    df_step2[col] = df_step2[col].apply(lambda x: x if isinstance(x, list) else to_list(x))

merged_events = []
mhid = 1

for _, row in df_step2.iterrows():

    merged = False

    for evt in merged_events:

        supplier_overlap = len(set(row["Suppliers"]) & set(evt["Suppliers"])) >= 1

        row_has_infra = len(row["Mills"]) > 0 or len(row["PIOConcessions"]) > 0
        evt_has_infra = len(evt["Mills"]) > 0 or len(evt["PIOConcessions"]) > 0

        infra_overlap = (
            len(set(row["Mills"]) & set(evt["Mills"])) >= 1 or
            len(set(row["PIOConcessions"]) & set(evt["PIOConcessions"])) >= 1
        )

        # ✅ New logic: MUST overlap in supplier AND infra
        if supplier_overlap and infra_overlap:

            evt["Suppliers"] = uniq_list(evt["Suppliers"] + row["Suppliers"])
            evt["Mills"] = uniq_list(evt["Mills"] + row["Mills"])
            evt["PIOConcessions"] = uniq_list(evt["PIOConcessions"] + row["PIOConcessions"])
            evt["Issues"] = uniq_list(evt["Issues"] + row["Issues"])
            evt["Source"] = uniq_list(evt["Source"] + row["Source"])
            evt["Grievance_List"] = uniq_list(evt["Grievance_List"] + row["Grievance_List"])

            evt["Grievance_Count"] = len(evt["Grievance_List"])
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
            "Grievance_Count": len(row["Grievance_List"])
        })
        mhid += 1

df_final = pd.DataFrame(merged_events)

# =====================================================
# STEP 4 – ADD GROUP INFO + AIRTABLE GROUPS
# =====================================================
print("\n[STEP 4] Adding Plot & Mill Groups...")

pio_file = "Concessions-v2-Grid view (5).csv"
mills_file = "Mills-Grid view (10).csv"

df_pio = pd.read_csv(pio_file, dtype=str)
df_mills = pd.read_csv(mills_file, dtype=str)

pio_group = pd.Series(df_pio["Group"].values, index=df_pio["ID"]).to_dict()
pio_airtable = pd.Series(df_pio["GroupAirtableRecID"].values, index=df_pio["ID"]).to_dict()

mills_group = pd.Series(df_mills["Group"].values, index=df_mills["UML_ID"]).to_dict()
mills_airtable = pd.Series(df_mills["GroupAirtableRecID"].values, index=df_mills["UML_ID"]).to_dict()

def get_groups(ids_list, mapping_dict):
    valid = []
    for i in ids_list:
        if i in mapping_dict and pd.notna(mapping_dict[i]):
            valid.append(str(mapping_dict[i]))
    return ", ".join(sorted(set(valid))) if valid else ""

df_final["Plot_Group"] = df_final["PIOConcessions"].apply(lambda x: get_groups(x, pio_group))
df_final["Plot_AirtableID_Group"] = df_final["PIOConcessions"].apply(lambda x: get_groups(x, pio_airtable))

df_final["Mill_Group"] = df_final["Mills"].apply(lambda x: get_groups(x, mills_group))
df_final["Mill_AirtableID_Group"] = df_final["Mills"].apply(lambda x: get_groups(x, mills_airtable))

print("✓ Added Plot & Mill group columns")

# =====================================================
# STEP 5 – COMPANY TRACKER LOOKUP (FROM ORIGINAL FILE)
# =====================================================
print("\n[STEP 5] Adding company tracker columns...")

df_lookup = pd.read_csv(INPUT_FILE, dtype=str)
df_lookup = df_lookup[["ID", "Company Tracker", "Tracker Company AirtableRecIDs"]]

# Ensure 'ID' column is unique before setting it as index for to_dict('index')
df_lookup = df_lookup.drop_duplicates(subset=['ID'], keep='first')

tracker_dict = df_lookup.set_index("ID")[["Company Tracker","Tracker Company AirtableRecIDs"]].to_dict("index")

def lookup_tracker(grievance_ids):
    companies = []
    rec_ids = []

    for gid in grievance_ids:
        if gid in tracker_dict:
            comp = tracker_dict[gid]["Company Tracker"]
            rec = tracker_dict[gid]["Tracker Company AirtableRecIDs"]

            if pd.notna(comp):
                companies.append(comp)
            if pd.notna(rec):
                rec_ids.append(rec)

    return (
        ", ".join(sorted(set(companies))),
        ", ".join(sorted(set(rec_ids)))
    )

df_final[["Company_Tracker", "Tracker_Company_AirtableRecIDs"]] = \
    df_final["Grievance_List"].apply(lambda x: pd.Series(lookup_tracker(x)))

print("✓ Added company tracker columns")

# =====================================================
# FINAL OUTPUT CLEANING
# =====================================================
for col in ["Suppliers","Mills","PIOConcessions","Issues","Source","Grievance_List"]:
    df_final[col] = df_final[col].apply(lambda x: ", ".join(sorted(set(x))))

df_final.to_csv(FINAL_OUT, index=False)

print("\n✅ FINAL DONE")
print("Final MHID count:", len(df_final))
print("Output saved to:", FINAL_OUT)
