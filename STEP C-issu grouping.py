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
df_original_grievances = pd.read_csv("Grievances-Grid view 2.csv", dtype=str)
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
        "Labor Rights Violations", "Child Labor", "Violence and/or Coercion",
        "Gender and Ethnic Disparities", "Human Rights Violation",
        "Labor Disputes", "Wage Dispute", "Forced Labor and/or Child Labor"
    ],
    "Land Conflict": [
        "Land Dispute", "Land Grabbing", "Indigenous Peoples Conflict",
        "Limited Access to Services"
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
            "Source": row["Source"]
           "Date Filed": row["Date Filed"]
            
        }
        final_rows.append(new_row)
        new_eid += 1

# Output akhir
df_step3 = pd.DataFrame(final_rows)
df_step3.to_csv("Step3.csv", index=False)

print("Step 3 selesai. Total events:", len(df_step3))
df_step3.head(40)

nah itu step 3, munculin lagi Date Filed nya
