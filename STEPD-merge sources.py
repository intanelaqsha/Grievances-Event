import pandas as pd
from datetime import datetime, timedelta

# =========================================
# LOAD Step 3
# =========================================
df3 = pd.read_csv("Step3.csv", dtype=str)
df3.columns = [c.strip() for c in df3.columns]

# Convert list-like columns ke Python list
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
# Step 4 – Merge events berdasarkan entitas + issue + time window
# =========================================
merged_events = []
mhid_id = 1

# Time window per Issue Category
def in_time_window(issue_cat, date_new, date_latest):
    if pd.isna(date_new) or pd.isna(date_latest):
        return False
    if issue_cat == "Environmental":
        return date_new.year == date_latest.year
    else:
        # 2 months range
        return abs((date_new - date_latest).days) <= 60

# Loop per Issue Category
for cat, group in df3.groupby("Issue_Category"):
    group = group.sort_values("Date_Filed").reset_index(drop=True)

    active_events = []

    for idx, row in group.iterrows():
        merged = False

        for evt in active_events:
            # Count entitas yang overlap
            ent_overlap = 0
            ent_overlap += len(set(row["Suppliers"]) & set(evt["Suppliers"]))
            ent_overlap += len(set(row["Mills"]) & set(evt["Mills"]))
            ent_overlap += len(set(row["PIOConcessions"]) & set(evt["PIOConcessions"]))

            # Check merge conditions: minimal 2 entitas + time window vs Latest_Date
            if ent_overlap >= 1 and in_time_window(cat, row["Date_Filed"], evt["Latest_Date"]):
                # Merge event
                evt["Suppliers"] = sorted(list(set(evt["Suppliers"]) | set(row["Suppliers"])))
                evt["Mills"] = sorted(list(set(evt["Mills"]) | set(row["Mills"])))
                evt["PIOConcessions"] = sorted(list(set(evt["PIOConcessions"]) | set(row["PIOConcessions"])))
                evt["Source"] = sorted(list(set(evt["Source"]) | set(row["Source"])))
                evt["Grievance_List"] = sorted(list(set(evt["Grievance_List"]) | set(row["Grievance_List"])))
                evt["Grievance_Count"] = len(evt["Grievance_List"])
                # Update earliest & latest Date Filed
                evt["Earliest_Date"] = min(evt["Earliest_Date"], row["Date_Filed"])
                evt["Latest_Date"] = max(evt["Latest_Date"], row["Date_Filed"])
                merged = True
                break

        # Jika tidak bisa merge → buat event baru
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

    # Tambahkan hasil per category ke merged_events
    merged_events.extend(active_events)

# Convert ke DataFrame
df_step4 = pd.DataFrame(merged_events)

# Convert list → string untuk CSV
for col in ["Suppliers", "Mills", "PIOConcessions", "Source", "Grievance_List"]:
    df_step4[col] = df_step4[col].apply(lambda x: ", ".join(x))

# Convert Date Filed ke string
df_step4["Earliest_Date"] = df_step4["Earliest_Date"].dt.strftime("%Y-%m-%d")
df_step4["Latest_Date"] = df_step4["Latest_Date"].dt.strftime("%Y-%m-%d")

# Save output
df_step4.to_csv("Step4_MergedEvents_Fix.csv", index=False)

print("Step 4 selesai. Total merged events:", len(df_step4))
df_step4.head(20)
