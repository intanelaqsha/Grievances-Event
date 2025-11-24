import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from collections import defaultdict

# ============================
# CONFIG
# ============================
SUPPLIER_WEIGHT = 0.30
MILLS_WEIGHT = 0.35
PIO_WEIGHT = 0.35

MERGE_THRESHOLD = 0.70
HIGH_THRESHOLD = 0.50

ENV_TIME_WINDOW = 90
OTHER_TIME_WINDOW = 60

model = SentenceTransformer("all-MiniLM-L6-v2")

# ============================
# LOAD DATA
# ============================
df = pd.read_csv("Step3.csv", dtype=str)

def to_list(cell):
    if pd.isna(cell) or cell.strip() == "":
        return []
    return [x.strip() for x in str(cell).split(",") if x.strip()]

for col in ["Suppliers", "Mills", "PIOConcessions", "Grievance_List"]:
    df[col] = df[col].apply(to_list)

df["Date_Filed"] = pd.to_datetime(df["Date_Filed"], errors="coerce")
df["Issue_Grouping"] = df["Issue_Grouping"].astype(str)
df["Issues"] = df["Issues"].astype(str)
df["Source"] = df["Source"].astype(str)
df["Event_ID_S3"] = df["Event_ID_S3"].astype(str)

# ============================
# FUNCTIONS
# ============================
def jaccard(a, b):
    a, b = set(a), set(b)
    if not a and not b:
        return 0
    return len(a & b) / len(a | b)

def entity_similarity(r1, r2):
    supplier_sim = jaccard(r1["Suppliers"], r2["Suppliers"])
    mills_sim = jaccard(r1["Mills"], r2["Mills"])
    pio_sim = jaccard(r1["PIOConcessions"], r2["PIOConcessions"])

    weighted_score = (
        supplier_sim * SUPPLIER_WEIGHT +
        mills_sim * MILLS_WEIGHT +
        pio_sim * PIO_WEIGHT
    )

    return weighted_score

def get_time_window(issue_group):
    if "enviro" in issue_group.lower():
        return ENV_TIME_WINDOW
    else:
        return OTHER_TIME_WINDOW

def time_diff_days(d1, d2):
    return abs((d1 - d2).days)

def build_text(row):
    return (
        f"{row['Source']} | "
        f"{row['Issues']} | "
        f"{' '.join(row['Suppliers'])} "
        f"{' '.join(row['Mills'])} "
        f"{' '.join(row['PIOConcessions'])}"
    )

# ============================
# EMBEDDINGS
# ============================
df["ai_text"] = df.apply(build_text, axis=1)
embeddings = model.encode(df["ai_text"].tolist(), normalize_embeddings=True)

# ============================
# STEP 4 ENGINE
# ============================
merged_events = []
used = set()
mhid_counter = 1

for group_name, group_df in df.groupby("Issue_Grouping"):
    group_df = group_df.sort_values("Date_Filed")
    indices = group_df.index.tolist()

    for i in range(len(indices)):
        base_idx = indices[i]
        if base_idx in used:
            continue

        base = df.loc[base_idx]
        base_emb = embeddings[base_idx]
        cluster = [base_idx]

        best_score = 0
        best_match = None

        for j in range(i + 1, len(indices)):
            comp_idx = indices[j]
            if comp_idx in used:
                continue

            comp = df.loc[comp_idx]

            days = time_diff_days(base["Date_Filed"], comp["Date_Filed"])
            window = get_time_window(base["Issue_Grouping"])

            # FOLLOW UP logic
            if base["Source"] == comp["Source"] and days > window:
                sim = cosine_similarity(
                    [base_emb],
                    [embeddings[comp_idx]]
                )[0][0]

                if sim > 0.85:
                    merged_events.append({
                        "base": base_idx,
                        "ref": comp_idx,
                        "level": "FOLLOW_UP"
                    })
                continue

            if days > window:
                continue

            ent_score = entity_similarity(base, comp)
            if ent_score == 0:
                continue

            total_score = ent_score

            if total_score > best_score:
                best_score = total_score
                best_match = comp["Event_ID_S3"]

            if total_score >= MERGE_THRESHOLD:
                cluster.append(comp_idx)

        # Determine level
        if best_score >= MERGE_THRESHOLD:
            level = "MERGE"
        elif best_score >= HIGH_THRESHOLD:
            level = "HIGH"
        else:
            level = "LOW"

        for idx in cluster:
            used.add(idx)

        merged_events.append({
            "cluster": cluster,
            "level": level,
            "merge_with": best_match
        })

# ============================
# BUILD OUTPUT
# ============================
final_rows = []

for i, evt in enumerate(merged_events):
    if "cluster" not in evt:
        continue

    sub = df.loc[evt["cluster"]]

    all_grievances = sorted(set(sum(sub["Grievance_List"].tolist(), [])))
    suppliers = sorted(set(sum(sub["Suppliers"].tolist(), [])))
    mills = sorted(set(sum(sub["Mills"].tolist(), [])))
    pios = sorted(set(sum(sub["PIOConcessions"].tolist(), [])))

    final_rows.append({
        "MHID": f"MHID{i+1:04d}",
        "Suppliers": ", ".join(suppliers),
        "Mills": ", ".join(mills),
        "PIOConcessions": ", ".join(pios),
        "Issue Grouping": sub["Issue_Grouping"].iloc[0],
        "Issue": ", ".join(sorted(set(sub["Issues"]))),
        "Grievance List": ", ".join(all_grievances),
        "Grievance Count": len(all_grievances),
        "Source": ", ".join(sorted(set(sub["Source"]))),
        "Earliest Date": sub["Date_Filed"].min(),
        "Latest Date": sub["Date_Filed"].max(),
        "Match Level": f"{evt['level']} {evt['merge_with']}" if evt["merge_with"] else evt["level"],
    })

final_df = pd.DataFrame(final_rows)
final_df.to_csv("STEP4_FINAL_EVENTS.csv", index=False)

print("✅ STEP 4 SELESAI")
print("Output file: STEP4_FINAL_EVENTS.csv")
print("Total MHID events:", len(final_df))
