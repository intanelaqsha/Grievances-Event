import pandas as pd

# ===== Ambil Step 2 result =====
events = df_step2.copy()

# Konversi list column → set (untuk mudah merge)
set_cols = ["Suppliers", "Mills", "PIOConcessions", "Issues", "Source", "Grievance_List"]
for col in set_cols:
    events[col] = events[col].apply(lambda x: set([i.strip() for i in str(x).split(",") if i.strip()]))

# Ambil Date Filed dari df_expanded (Step 1)
date_map = (
    df_expanded[["Raw_ID", "Source", "Date Filed"]]
    .drop_duplicates()
    .set_index(["Raw_ID", "Source"])["Date Filed"]
    .to_dict()
)

# Tambah kolom Date_Filed berdasarkan Raw_ID & Source
events["Date_Filed"] = events.apply(
    lambda r: set([
        date_map.get((rid, src))
        for rid in df_expanded["Raw_ID"].unique()
        for src in r["Source"]
        if (rid, src) in date_map
    ]),
    axis=1
)

events_list = events.to_dict("records")

# ===== STEP 3 MERGING =====
merged = [False] * len(events_list)

for i in range(len(events_list)):
    if merged[i]:
        continue

    A = events_list[i]

    for j in range(i+1, len(events_list)):
        if merged[j]:
            continue

        B = events_list[j]

        # 1. Harus sama tanggal
        if len(A["Date_Filed"] & B["Date_Filed"]) == 0:
            continue

        # 2. Harus beda source
        if len(A["Source"] & B["Source"]) > 0:
            continue

        # 3. Harus ada overlap minimal 1 entitas
        overlap = (
            len(A["Suppliers"] & B["Suppliers"]) > 0 or
            len(A["Mills"] & B["Mills"]) > 0 or
            len(A["PIOConcessions"] & B["PIOConcessions"]) > 0
        )

        if not overlap:
            continue

        # ===== MERGE B → A =====
        A["Suppliers"] |= B["Suppliers"]
        A["Mills"] |= B["Mills"]
        A["PIOConcessions"] |= B["PIOConcessions"]
        A["Issues"] |= B["Issues"]
        A["Source"] |= B["Source"]

        # Merge grievance ID list
        A["Grievance_List"] |= B["Grievance_List"]
        A["Grievance_Count"] = len(A["Grievance_List"])

        # Tanggal tetap
        A["Date_Filed"] |= B["Date_Filed"]

        merged[j] = True

# ===== Final Output =====
final = [events_list[k] for k in range(len(events_list)) if not merged[k]]

def to_str(s):
    return ", ".join(sorted(list(s)))

final_df = pd.DataFrame([
    {
        "Event_ID": e["Event_ID"],
        "Suppliers": to_str(e["Suppliers"]),
        "Mills": to_str(e["Mills"]),
        "PIOConcessions": to_str(e["PIOConcessions"]),
        "Issues": to_str(e["Issues"]),
        "Source": to_str(e["Source"]),
        "Grievance_List": to_str(e["Grievance_List"]),
        "Grievance_Count": e["Grievance_Count"],
    }
    for e in final
])

final_df.to_csv("Step3_output.csv", index=False)
final_df.head(20)
len(final_df)
