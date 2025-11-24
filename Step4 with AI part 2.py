#if sourcr same and entity merge, merge without time window concern


import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

# ============================
# CONFIG
# ============================
PIO_WEIGHT = 0.35
MILLS_WEIGHT = 0.35
SUPPLIER_WEIGHT = 0.30

MERGE_THRESHOLD = 0.70
HIGH_THRESHOLD  = 0.50

AI_OVERRIDE_THRESHOLD = 0.85
ENV_WINDOW = 90
OTHER_WINDOW = 60

model = SentenceTransformer("all-MiniLM-L6-v2")

# ============================
# LOAD DATA
# ============================
df = pd.read_csv("Step3.csv", dtype=str)

def to_list(cell):
    if pd.isna(cell) or cell.strip() == "":
        return []
    return [x.strip() for x in cell.split(",") if x.strip()]

for col in ["Suppliers", "Mills", "PIOConcessions", "Grievance_List"]:
    df[col] = df[col].apply(to_list)

df["Date_Filed"] = pd.to_datetime(df["Date_Filed"], errors="coerce")
df["Issue_Category"] = df["Issue_Category"].astype(str)
df["Issues"] = df["Issues"].astype(str)
df["Source"] = df["Source"].astype(str)

# ============================
# SIMILARITY FUNCTIONS
# ============================
def jaccard(a, b):
    A, B = set(a), set(b)
    if not A and not B:
        return 0.0
    return len(A & B) / len(A | B)

def entity_weighted_similarity(r1, r2):
    pio_sim = jaccard(r1["PIOConcessions"], r2["PIOConcessions"])
    mills_sim = jaccard(r1["Mills"], r2["Mills"])
    supplier_sim = jaccard(r1["Suppliers"], r2["Suppliers"])

    weighted = (
        pio_sim * PIO_WEIGHT +
        mills_sim * MILLS_WEIGHT +
        supplier_sim * SUPPLIER_WEIGHT
    )
    return weighted

def get_time_window(issue_group):
    if issue_group.lower().strip() == "environmental":
        return ENV_WINDOW
    else:
        return OTHER_WINDOW

def time_in_window(d1, d2, issue_group):
    window = get_time_window(issue_group)
    return abs((d1 - d2).days) <= window

def build_text(row):
    ent = " ".join(row["Suppliers"] + row["Mills"] + row["PIOConcessions"])
    return f"{row['Source']} | {row['Issues']} | {ent}"

def ai_similarity(text1, text2):
    emb = model.encode([text1, text2], normalize_embeddings=True)
    return cosine_similarity([emb[0]], [emb[1]])[0][0]

# ============================
# PRECOMPUTE AI TEXT
# ============================
df["ai_text"] = df.apply(build_text, axis=1)

# ============================
# MATCHING ENGINE
# ============================
used = set()
clusters = []

grouped = df.groupby("Issue_Category")

for issue_group, group in grouped:
    group = group.sort_values("Date_Filed")
    idx_list = group.index.tolist()

    for i, idx_i in enumerate(idx_list):
        if idx_i in used:
            continue

        base = df.loc[idx_i]
        cluster = [idx_i]
        merge_with = None
        best_score = 0

        for j in range(i+1, len(idx_list)):
            idx_j = idx_list[j]
            if idx_j in used:
                continue

            comp = df.loc[idx_j]

            # =====================
            # SOURCE OVERRIDE
            # =====================
            if base["Source"] == comp["Source"]:
                ai_sim = ai_similarity(base["ai_text"], comp["ai_text"])
                if ai_sim >= AI_OVERRIDE_THRESHOLD:
                    cluster.append(idx_j)
                    continue

            # =====================
            # TIME FILTER NORMAL
            # =====================
            if not time_in_window(base["Date_Filed"], comp["Date_Filed"], issue_group):
                continue

            # =====================
            # ENTITY WEIGHTED SCORE
            # =====================
            ent_score = entity_weighted_similarity(base, comp)
            ai_sim = ai_similarity(base["ai_text"], comp["ai_text"])

            final_score = 0.7 * ent_score + 0.3 * ai_sim

            if final_score > best_score:
                best_score = final_score
                merge_with = comp["Event_ID_S3"]

            if final_score >= MERGE_THRESHOLD:
                cluster.append(idx_j)

        for idx in cluster:
            used.add(idx)

        if best_score >= MERGE_THRESHOLD:
            level = f"MERGE {merge_with}"
        elif best_score >= HIGH_THRESHOLD:
            level = f"HIGH {merge_with}"
        else:
            level = f"LOW {merge_with}"

        clusters.append({
            "cluster": cluster,
            "match_level": level
        })

# ============================
# BUILD FINAL OUTPUT
# ============================
final_data = []

for i, item in enumerate(clusters):
    cluster = item["cluster"]
    sub = df.loc[cluster]

    def unique_flat(col):
        return sorted(set(sum(sub[col].tolist(), [])))

    final_data.append({
        "MHID": f"MHID{i+1:04d}",
        "Suppliers": ", ".join(unique_flat("Suppliers")),
        "Mills": ", ".join(unique_flat("Mills")),
        "PIOConcessions": ", ".join(unique_flat("PIOConcessions")),
        "Issue_Group": sub["Issue_Category"].iloc[0],
        "Issue": ", ".join(sorted(set(sub["Issues"].tolist()))),
        "Grievance_List": ", ".join(unique_flat("Grievance_List")),
        "Grievance_Count": len(unique_flat("Grievance_List")),
        "Source": ", ".join(sorted(set(sub["Source"].tolist()))),
        "Earliest_Date": sub["Date_Filed"].min(),
        "Latest_Date": sub["Date_Filed"].max(),
        "Match_Level + Merge_With": item["match_level"]
    })

final_df = pd.DataFrame(final_data)
final_df.to_csv("EVENT_MERGED_FINAL.csv", index=False)

print("✅ STEP 4 SELESAI")
print("Total MHID:", len(final_df))
print("Output: EVENT_MERGED_FINAL.csv")
