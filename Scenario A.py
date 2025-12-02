#Directly Merge if Sources + mills/plot overlap. 

import pandas as pd
import re

# CONFIG / INPUT FILES
INPUT_FILE = "Grievances-Grid view 3.csv"
PIO_FILE = "Concessions-v2-Grid view (5).csv"
MILLS_FILE = "Mills-Grid view (10).csv"
OUT_FILE = "Merged_Events_with_groups.csv"

# =====================================================
# LOAD CSV
# =====================================================
df = pd.read_csv(INPUT_FILE, dtype=str)
df.columns = [c.strip() for c in df.columns]
df["Raw_ID"] = df.index.astype(int)

# =====================================================
# HELPERS: splitters + util
# =====================================================
def split_list(cell):
    """Generic splitter for suppliers/mills/PiOConcessions/issues"""
    if pd.isna(cell) or str(cell).strip() == "":
        return []
    s = str(cell).replace("[", "").replace("]", "")
    parts = [p.strip() for p in re.split("[,;]", s)]
    return [p for p in sorted(set(parts)) if p]

def split_source(val):
    """Special splitter for Source: split on comma NOT followed by space"""
    if pd.isna(val) or str(val).strip() == "":
        return []
    s = str(val).strip()
    parts = re.split(r",(?!\s)", s)   # split on comma NOT followed by a space
    parts = [p.strip() for p in parts if p.strip()]
    return parts

def uniq_list(x):
    return sorted(list(set(x)))

# ensure columns exist
for c in ["Suppliers","Mills","PIOConcessions-v2","Issues","Source","ID","Company Tracker","Tracker Company AirtableRecIDs"]:
    if c not in df.columns:
        df[c] = ""

# =====================================================
# APPLY SPLITTERS
# =====================================================
multi_cols = ["Suppliers", "Mills", "PIOConcessions-v2", "Issues"]
for col in multi_cols:
    df[col] = df[col].apply(split_list)

df["Source"] = df["Source"].apply(split_source)

# =====================================================
# STEP 2 — MERGE EVENTS (SOURCE + (MILLS OR PIO) overlap)
# =====================================================
events = []
event_id = 1

for idx, row in df.iterrows():
    sup = set(row["Suppliers"])
    mil = set(row["Mills"])
    pio = set(row["PIOConcessions-v2"])
    iss = set(row["Issues"])
    sources = set(row["Source"])
    gid = row["ID"] if pd.notna(row.get("ID")) else f"ROW_{idx}"

    merged = False

    for evt in events:
        evt_sources = set(evt["Sources"])
        evt_mills = set(evt["Mills"])
        evt_pios = set(evt["PIOConcessions-v2"])

        source_overlap = len(sources & evt_sources) > 0
        mill_overlap = len(mil & evt_mills) > 0
        pio_overlap = len(pio & evt_pios) > 0

        allow_merge = source_overlap and (mill_overlap or pio_overlap)

        if allow_merge:
            # merge sets
            evt["Suppliers"] = uniq_list(set(evt["Suppliers"]) | sup)
            evt["Mills"] = uniq_list(set(evt["Mills"]) | mil)
            evt["PIOConcessions-v2"] = uniq_list(set(evt["PIOConcessions-v2"]) | pio)
            evt["Issues"] = uniq_list(set(evt["Issues"]) | iss)
            evt["Sources"] = uniq_list(set(evt["Sources"]) | sources)

            # merge grievance list
            evt["Grievance_List"].append(gid)
            evt["Grievance_List"] = uniq_list(evt["Grievance_List"])
            evt["Grievance_Count"] = len(evt["Grievance_List"])

            merged = True
            break

    if not merged:
        events.append({
            "Event_ID": f"EVT_{event_id}",
            "Suppliers": uniq_list(list(sup)),
            "Mills": uniq_list(list(mil)),
            "PIOConcessions-v2": uniq_list(list(pio)),
            "Issues": uniq_list(list(iss)),
            "Sources": uniq_list(list(sources)),
            "Grievance_List": [gid],
            "Grievance_Count": 1
        })
        event_id += 1

df_final = pd.DataFrame(events)

# =====================================================
# STEP 4 — ADD PLOT & MILL GROUP LOOKUPS
# =====================================================
# load lookup tables
df_pio = pd.read_csv(PIO_FILE, dtype=str)
df_mills = pd.read_csv(MILLS_FILE, dtype=str)

# build mapping dicts
pio_group = pd.Series(df_pio["Group"].values, index=df_pio["ID"]).to_dict() if "ID" in df_pio.columns and "Group" in df_pio.columns else {}
pio_airtable = pd.Series(df_pio["GroupAirtableRecID"].values, index=df_pio["ID"]).to_dict() if "ID" in df_pio.columns and "GroupAirtableRecID" in df_pio.columns else {}

mills_group = pd.Series(df_mills["Group"].values, index=df_mills["UML_ID"]).to_dict() if "UML_ID" in df_mills.columns and "Group" in df_mills.columns else {}
mills_airtable = pd.Series(df_mills["GroupAirtableRecID"].values, index=df_mills["UML_ID"]).to_dict() if "UML_ID" in df_mills.columns and "GroupAirtableRecID" in df_mills.columns else {}

def get_groups_from_ids(ids_list, mapping_dict):
    valid = []
    for i in ids_list:
        if i in mapping_dict and pd.notna(mapping_dict[i]):
            valid.append(str(mapping_dict[i]))
    return ", ".join(sorted(set(valid))) if valid else ""

# apply lookups (note: df_final cols currently lists)
df_final["Plot_Group"] = df_final["PIOConcessions-v2"].apply(lambda x: get_groups_from_ids(x, pio_group))
df_final["Plot_AirtableID_Group"] = df_final["PIOConcessions-v2"].apply(lambda x: get_groups_from_ids(x, pio_airtable))

df_final["Mill_Group"] = df_final["Mills"].apply(lambda x: get_groups_from_ids(x, mills_group))
df_final["Mill_AirtableID_Group"] = df_final["Mills"].apply(lambda x: get_groups_from_ids(x, mills_airtable))

# =====================================================
# STEP 5 — COMPANY TRACKER LOOKUP (FROM ORIGINAL FILE)
# =====================================================
# build tracker dict from original input file (ID -> Company Tracker, Tracker Company AirtableRecIDs)
df_lookup = pd.read_csv(INPUT_FILE, dtype=str)
for c in ["ID","Company Tracker","Tracker Company AirtableRecIDs"]:
    if c not in df_lookup.columns:
        df_lookup[c] = ""

df_lookup = df_lookup.drop_duplicates(subset=['ID'], keep='first')
tracker_dict = df_lookup.set_index("ID")[["Company Tracker","Tracker Company AirtableRecIDs"]].to_dict("index")

def lookup_tracker(grievance_ids):
    companies = []
    rec_ids = []
    for gid in grievance_ids:
        if gid in tracker_dict:
            comp = tracker_dict[gid].get("Company Tracker")
            rec = tracker_dict[gid].get("Tracker Company AirtableRecIDs")
            if pd.notna(comp) and str(comp).strip() != "":
                companies.append(comp)
            if pd.notna(rec) and str(rec).strip() != "":
                rec_ids.append(rec)
    return ", ".join(sorted(set(companies))), ", ".join(sorted(set(rec_ids)))

df_final[["Company_Tracker", "Tracker_Company_AirtableRecIDs"]] = df_final["Grievance_List"].apply(lambda x: pd.Series(lookup_tracker(x)))

# =====================================================
# FINAL CLEANUP & SAVE
# =====================================================
# Convert list columns to comma-separated strings for CSV
list_cols = ["Suppliers", "Mills", "PIOConcessions-v2", "Issues", "Sources", "Grievance_List"]
for col in list_cols:
    df_final[col] = df_final[col].apply(lambda v: ", ".join(v) if isinstance(v, (list, set)) else (v if pd.notna(v) else ""))

# reorder columns for readability
cols_order = ["Event_ID","Suppliers","Mills","PIOConcessions-v2","Plot_Group","Plot_AirtableID_Group",
              "Mill_Group","Mill_AirtableID_Group","Sources","Grievance_List","Grievance_Count",
              "Company_Tracker","Tracker_Company_AirtableRecIDs"]
cols_order = [c for c in cols_order if c in df_final.columns] + [c for c in df_final.columns if c not in cols_order]
df_final = df_final[cols_order]

df_final.to_csv(OUT_FILE, index=False)

print("TOTAL EVENTS:", df_final.shape[0])
df_final.head(20)
