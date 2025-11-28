#Split issues, if have deforestation...merge and no time window.
#any grievances without deforestation/peatland loss, timw window 90

import pandas as pd
import re
from datetime import datetime

# =====================================================
# CONFIG
# =====================================================
INPUT_FILE = "Grievances-Grid view 3.csv"
FINAL_OUT = "Final_Merged_splitissue_splittime.csv"
TIME_WINDOW_DAYS = 90 

# =====================================================
# HELPERS
# =====================================================
def split_list(cell):
    if pd.isna(cell) or str(cell).strip() == "":
        return []
    s = str(cell).replace("[", "").replace("]", "")
    parts = [p.strip() for p in re.split("[,;]", s)]
    return [p for p in parts if p and p.lower() not in ("nan","none")]

def split_source(val):
    if pd.isna(val) or str(val).strip() == "":
        return []
    parts = re.split(r",(?![\s])", str(val))
    return [p.strip() for p in parts if p.strip() and p.lower() not in ("nan","none")]

def normalize_list(lst):
    out = []
    for v in lst or []:
        if pd.isna(v):
            continue
        s = str(v).strip()
        if s == "" or s.lower() in ("nan","none"):
            continue
        out.append(s)
    return out

def uniq_list(x):
    return sorted(list(dict.fromkeys(x)))

def parse_date_safe(x):
    try:
        return pd.to_datetime(x, errors="coerce")
    except Exception:
        return pd.NaT

def time_overlap(d1, d2, window_days):
    if pd.isna(d1) or pd.isna(d2):
        return False
    return abs((d1 - d2).days) <= window_days

def contains_deforestation_or_peat(issues):
    for i in issues or []:
        if not i: continue
        s = str(i).lower()
        if "deforest" in s or "peat" in s:
            return True
    return False

# =====================================================
# STEP 1 – LOAD + NORMALIZE + EXPAND SOURCE
# =====================================================
print("[STEP 1] Load + Normalize")

df = pd.read_csv(INPUT_FILE, dtype=str)
df.columns = [c.strip() for c in df.columns]
df["Raw_ID"] = df.index.astype(int)

multi_cols = ["Suppliers", "Mills", "PIOConcessions", "Issues"]
for col in multi_cols:
    df[col] = df[col].apply(split_list)

df["Source"] = df["Source"].apply(split_source)

expanded = []
for _, r in df.iterrows():
    sources = r["Source"]
    if not sources:
        new = r.copy()
        new["Source"] = []
        expanded.append(new)
    else:
        for s in sources:
            new = r.copy()
            new["Source"] = [s]
            expanded.append(new)

df_expanded = pd.DataFrame(expanded).reset_index(drop=True)
print("Expanded rows:", len(df_expanded))


# =====================================================
# STEP 2 – MERGE PER SOURCE (initial clustering)
# =====================================================
print("[STEP 2] Merge per Source")

events = []
evt_id = 1

# group by first source value (or None)
group_keys = df_expanded["Source"].apply(lambda x: x[0] if isinstance(x, list) and x else None)
for key, grp in df_expanded.groupby(group_keys):
    source_events = []
    for _, row in grp.iterrows():
        sup = set(normalize_list(row.get("Suppliers", [])))
        mil = set(normalize_list(row.get("Mills", [])))
        pio = set(normalize_list(row.get("PIOConcessions", [])))
        iss = normalize_list(row.get("Issues", []))
        gid_raw = row.get("ID", "")
        gid = str(gid_raw).strip() if pd.notna(gid_raw) else ""
        if gid == "":
            # fallback: use Raw_ID for traceability
            gid = f"RAW_{row.get('Raw_ID','')}"
        date_filed_raw = row.get("Date Filed", "")
        date_filed = str(date_filed_raw).strip()

        merged = False
        for evt in source_events:
            evt_sup = set(normalize_list(evt["Suppliers"]))
            evt_mil = set(normalize_list(evt["Mills"]))
            evt_pio = set(normalize_list(evt["PIOConcessions"]))

            overlap = bool(sup & evt_sup) or bool(mil & evt_mil) or bool(pio & evt_pio)
            if overlap:
                evt["Suppliers"] = uniq_list(evt["Suppliers"] + list(sup))
                evt["Mills"] = uniq_list(evt["Mills"] + list(mil))
                evt["PIOConcessions"] = uniq_list(evt["PIOConcessions"] + list(pio))
                evt["Issues"] = uniq_list(evt["Issues"] + iss)
                evt["Grievance_List"].append(gid)
                evt["Date_Filed_List"].append(date_filed)
                merged = True
                break
        if not merged:
            source_events.append({
                "Event_ID": f"EVT_{evt_id}",
                "Source": [key] if key is not None else [],
                "Suppliers": list(sup),
                "Mills": list(mil),
                "PIOConcessions": list(pio),
                "Issues": iss,
                "Grievance_List": [gid],
                "Date_Filed_List": [date_filed]
            })
            evt_id += 1
    events.extend(source_events)

df_step2 = pd.DataFrame(events)
print("Step2 events:", len(df_step2))

# =====================================================
# STEP 2.5 – SPLIT GROUP A / B berdasarkan Issues
# =====================================================
df_step2["Issues"] = df_step2["Issues"].apply(lambda x: normalize_list(x))
df_step2["Issue_Group"] = df_step2["Issues"].apply(lambda x: "A" if contains_deforestation_or_peat(x) else "B")

groupA = df_step2[df_step2["Issue_Group"] == "A"].copy()
groupB = df_step2[df_step2["Issue_Group"] == "B"].copy()
print("Group A:", len(groupA), "Group B:", len(groupB))

# =====================================================
# STEP 3 – FINAL MERGE (different rules per group)
# =====================================================
def finalize_merge(df_input, use_time_window):
    # prepare date fields
    def earliest_date(list_dates):
        arr = [parse_date_safe(d) for d in (list_dates or []) if str(d).strip()!=""]
        arr = [a for a in arr if not pd.isna(a)]
        return min(arr) if arr else pd.NaT

    rows = df_input.to_dict("records")
    # normalize rows in-place
    norm_rows = []
    for r in rows:
        nr = {}
        nr["Suppliers"] = normalize_list(r.get("Suppliers", []))
        nr["Mills"] = normalize_list(r.get("Mills", []))
        nr["PIOConcessions"] = normalize_list(r.get("PIOConcessions", []))
        nr["Issues"] = normalize_list(r.get("Issues", []))
        # ensure grievance list items are strings trimmed
        nr["Grievance_List"] = [str(x).strip() for x in r.get("Grievance_List", []) if str(x).strip() and str(x).strip().lower() not in ("nan","none")]
        nr["Date_Filed_dt"] = earliest_date(r.get("Date_Filed_List", []))
        nr["Source"] = normalize_list(r.get("Source", []))
        norm_rows.append(nr)

    merged = []
    for r in norm_rows:
        did_merge = False
        for evt in merged:
            evt_sup = set(evt["Suppliers"])
            evt_mil = set(evt["Mills"])
            evt_pio = set(evt["PIOConcessions"])

            supplier_overlap = bool(set(r["Suppliers"]) & evt_sup)

            row_has_infra = bool(r["Mills"]) or bool(r["PIOConcessions"])
            evt_has_infra = bool(evt["Mills"]) or bool(evt["PIOConcessions"])

            infra_overlap = bool(set(r["Mills"]) & evt_mil) or bool(set(r["PIOConcessions"]) & evt_pio)

            # determine time_ok if needed
            time_ok = True
            if use_time_window:
                evt_latest = evt.get("Latest_Date_dt", pd.NaT)
                time_ok = time_overlap(r["Date_Filed_dt"], evt_latest, TIME_WINDOW_DAYS)

            # Merge rules:
            # - If both have infra => require supplier_overlap AND infra_overlap (and time_ok if required)
            # - Else (one/both missing infra) => require supplier_overlap (and time_ok if required)
            if not supplier_overlap:
                continue

            if row_has_infra and evt_has_infra:
                if not infra_overlap:
                    continue
            # else: supplier_overlap is enough

            if use_time_window and not time_ok:
                continue

            # perform merge
            evt["Suppliers"] = uniq_list(evt["Suppliers"] + r["Suppliers"])
            evt["Mills"] = uniq_list(evt["Mills"] + r["Mills"])
            evt["PIOConcessions"] = uniq_list(evt["PIOConcessions"] + r["PIOConcessions"])
            evt["Issues"] = uniq_list(evt["Issues"] + r["Issues"])
            evt["Source"] = uniq_list((evt.get("Source") or []) + r.get("Source", []))
            evt["Grievance_List"] = uniq_list(evt["Grievance_List"] + r["Grievance_List"])
            # update dates
            all_dates = [d for d in [evt.get("Earliest_Date_dt", pd.NaT), evt.get("Latest_Date_dt", pd.NaT), r.get("Date_Filed_dt")] if not pd.isna(d)]
            if all_dates:
                evt["Earliest_Date_dt"] = min(all_dates)
                evt["Latest_Date_dt"] = max(all_dates)
            else:
                evt["Earliest_Date_dt"] = pd.NaT
                evt["Latest_Date_dt"] = pd.NaT

            did_merge = True
            break

        if not did_merge:
            merged.append({
                # DO NOT assign MHID here to avoid duplicates across groups
                "Suppliers": list(r["Suppliers"]),
                "Mills": list(r["Mills"]),
                "PIOConcessions": list(r["PIOConcessions"]),
                "Issues": list(r["Issues"]),
                "Source": list(r.get("Source", [])),
                "Grievance_List": list(r["Grievance_List"]),
                "Earliest_Date_dt": r.get("Date_Filed_dt", pd.NaT),
                "Latest_Date_dt": r.get("Date_Filed_dt", pd.NaT)
            })

    return merged

merged_A = finalize_merge(groupA, use_time_window=False)  # ignore time
merged_B = finalize_merge(groupB, use_time_window=True)   # respect time window

# =====================================================
# REASSIGN GLOBAL UNIQUE MHID (fix duplicates)
# =====================================================
all_events = merged_A + merged_B

# ensure each event is normalized lists (safety)
for e in all_events:
    for k in ["Suppliers","Mills","PIOConcessions","Issues","Source","Grievance_List"]:
        e[k] = uniq_list(normalize_list(e.get(k, [])))

# assign unique MHID sequentially
for i, e in enumerate(all_events, start=1):
    e["MHID"] = f"MHID_{i:03d}"  # zero-padded, change padding if you want

# convert to dataframe now
df_final = pd.DataFrame(all_events)
print("Final merged events:", len(df_final))

# =====================================================
# STEP 4 – GROUP MAPPING + AIRTABLE IDs
# =====================================================
print("[STEP 4] Map groups & airtable IDs")

pio_file = "Concessions-v2-Grid view (5).csv"
mills_file = "Mills-Grid view (10).csv"

df_pio = pd.read_csv(pio_file, dtype=str)
df_mills = pd.read_csv(mills_file, dtype=str)

pio_group = pd.Series(df_pio["Group"].values, index=df_pio["ID"]).to_dict()
pio_air = pd.Series(df_pio["GroupAirtableRecID"].values, index=df_pio["ID"]).to_dict()

mills_group = pd.Series(df_mills["Group"].values, index=df_mills["UML_ID"]).to_dict()
mills_air = pd.Series(df_mills["GroupAirtableRecID"].values, index=df_mills["UML_ID"]).to_dict()

def map_group_ids(ids, mapping):
    out = []
    for i in ids or []:
        if not i: continue
        key = str(i).strip()
        if key in mapping and pd.notna(mapping[key]):
            out.append(str(mapping[key]))
    return ", ".join(sorted(set(out))) if out else ""

# map; these expect list inputs, ensure lists exist
df_final["Plot_Group"] = df_final["PIOConcessions"].apply(lambda x: map_group_ids(x, pio_group))
df_final["Plot_AirtableID_Group"] = df_final["PIOConcessions"].apply(lambda x: map_group_ids(x, pio_air))
df_final["Mill_Group"] = df_final["Mills"].apply(lambda x: map_group_ids(x, mills_group))
df_final["Mill_AirtableID_Group"] = df_final["Mills"].apply(lambda x: map_group_ids(x, mills_air))

# =====================================================
# STEP 5 – COMPANY TRACKER LOOKUP
# =====================================================
print("[STEP 5] Company tracker lookup")

tracker_df = pd.read_csv(INPUT_FILE, dtype=str)
if "ID" in tracker_df.columns:
    tracker_df["ID"] = tracker_df["ID"].astype(str).str.strip()
    tracker_df = tracker_df.drop_duplicates(subset=['ID'], keep='first').set_index("ID")
else:
    # create empty tracker df if ID missing
    tracker_df = pd.DataFrame(columns=["Company Tracker","Tracker Company AirtableRecIDs"]).set_index(pd.Index([], name="ID"))

def lookup_tracker_fields(griev_list):
    comps, recids = [], []
    for gid in griev_list or []:
        g = str(gid).strip()
        if g in tracker_df.index:
            comp_val = tracker_df.at[g, "Company Tracker"] if "Company Tracker" in tracker_df.columns else ""
            rec_val = tracker_df.at[g, "Tracker Company AirtableRecIDs"] if "Tracker Company AirtableRecIDs" in tracker_df.columns else ""
            comp_str = str(comp_val).strip() if pd.notna(comp_val) else ""
            rec_str = str(rec_val).strip() if pd.notna(rec_val) else ""
            if comp_str:
                comps.append(comp_str)
            if rec_str:
                recids.append(rec_str)
    return ", ".join(sorted(set(comps))), ", ".join(sorted(set(recids)))

if not df_final.empty:
    df_final[["Company_Tracker","Tracker_Company_AirtableRecIDs"]] = df_final["Grievance_List"].apply(lambda x: pd.Series(lookup_tracker_fields(x)))
else:
    df_final["Company_Tracker"] = ""
    df_final["Tracker_Company_AirtableRecIDs"] = ""

# =====================================================
# STEP 5.5 – ADD GRIEVANCE COUNT (unique)
# =====================================================
print("[STEP 5.5] Adding grievance count")
def count_grievances(griev_list):
    if not isinstance(griev_list, list):
        return 0
    clean = [str(g).strip() for g in griev_list if str(g).strip() not in ("", "nan", "None")]
    return len(set(clean))

df_final["Grievance_Count"] = df_final["Grievance_List"].apply(count_grievances)

# =====================================================
# FINAL CLEAN & EXPORT
# =====================================================
# turn list columns into comma-separated strings for export
list_cols = ["Suppliers","Mills","PIOConcessions","Issues","Source","Grievance_List"]
for col in list_cols:
    if col in df_final.columns:
        df_final[col] = df_final[col].apply(lambda x: ", ".join(sorted(set([str(i).strip() for i in x]))) if isinstance(x, list) else (str(x) if pd.notna(x) else ""))

if "Earliest_Date_dt" in df_final.columns:
    df_final["Earliest_Date"] = pd.to_datetime(df_final["Earliest_Date_dt"], errors="coerce").dt.strftime("%Y-%m-%d")
else:
    df_final["Earliest_Date"] = ""

if "Latest_Date_dt" in df_final.columns:
    df_final["Latest_Date"] = pd.to_datetime(df_final["Latest_Date_dt"], errors="coerce").dt.strftime("%Y-%m-%d")
else:
    df_final["Latest_Date"] = ""

# drop helpers
for c in ["Earliest_Date_dt","Latest_Date_dt"]:
    if c in df_final.columns:
        df_final.drop(columns=[c], inplace=True)

# ensure MHID first column for readability
cols = df_final.columns.tolist()
if "MHID" in cols:
    cols = ["MHID"] + [c for c in cols if c != "MHID"]
    df_final = df_final[cols]

df_final.to_csv(FINAL_OUT, index=False)
print("Saved:", FINAL_OUT)
print("Total MHID:", len(df_final))
