# --- FIXED EVENT CLUSTERING PER SOURCE (NO BLEEDING) ---
import pandas as pd
from collections import defaultdict
import itertools
import re
import os

# ========== LOAD FILE ==========
file_path = "/content/Grievances-Grid view.csv"

df = pd.read_csv(file_path, dtype=str)
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

# required columns
for col in ["id", "suppliers", "source", "mills", "pioconcessions", "issues"]:
    if col not in df.columns:
        df[col] = ""


# ========== CLEANING HELPERS ==========
def split_items(val):
    if pd.isna(val) or val == "":
        return []
    parts = re.split(r"[;,]\s*", str(val))
    return [p.strip() for p in parts if p.strip()]


def normalize(x):
    return re.sub(r"\s+", " ", str(x).strip()).upper() if pd.notna(x) else ""


# Expand multiple sources per grievance
expanded = []
for _, row in df.iterrows():
    sources = split_items(row["source"])
    if not sources:
        sources = [""]

    for s in sources:
        new_row = row.copy()
        new_row["source_single"] = normalize(s)
        new_row["suppliers_list"] = [normalize(i) for i in split_items(row["suppliers"])]
        new_row["plots_list"] = [normalize(i) for i in split_items(row["pioconcessions"])]
        new_row["mills_list"] = [normalize(i) for i in split_items(row["mills"])]
        new_row["issues_list"] = [normalize(i) for i in split_items(row["issues"])]
        expanded.append(new_row)

exp = pd.DataFrame(expanded)


# ========== BUILD CLUSTERS PER SOURCE ==========
def build_clusters_for_source(group):

    # Step 1: build adjacency list
    adj = defaultdict(set)

    for _, r in group.iterrows():
        rid = r["id"]

        # supplier-level matches
        for sup in r["suppliers_list"]:
            if sup:
                adj[f"SUP_{sup}"].add(rid)

        # plot matches
        for p in r["plots_list"]:
            if p:
                adj[f"PLOT_{p}"].add(rid)

        # mill matches
        for m in r["mills_list"]:
            if m:
                adj[f"MILL_{m}"].add(rid)

    # Step 2: connected components per source only
    # build graph edges ONLY among IDs in this source
    graph = defaultdict(set)
    all_ids = set(group["id"].tolist())

    for entity, ids in adj.items():
        ids = list(ids)
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                a, b = ids[i], ids[j]
                if a in all_ids and b in all_ids:
                    graph[a].add(b)
                    graph[b].add(a)

    # find connected components
    visited = set()
    clusters = []

    for node in all_ids:
        if node not in visited:
            stack = [node]
            comp = set()

            while stack:
                x = stack.pop()
                if x not in visited:
                    visited.add(x)
                    comp.add(x)
                    for nx in graph[x]:
                        if nx not in visited:
                            stack.append(nx)

            clusters.append(comp)

    return clusters


# ========== GENERATE EVENT TABLE ==========
event_rows = []
event_id_counter = itertools.count(1)

for source, group in exp.groupby("source_single"):
    clusters = build_clusters_for_source(group)

    for comp in clusters:

        comp_rows = group[group["id"].isin(comp)]

        all_sup = sorted({s for lst in comp_rows["suppliers_list"] for s in lst})
        all_plots = sorted({p for lst in comp_rows["plots_list"] for p in lst})
        all_mills = sorted({m for lst in comp_rows["mills_list"] for m in lst})
        all_issues = sorted({i for lst in comp_rows["issues_list"] for i in lst})

        event_rows.append({
            "event_id": f"EVT-{next(event_id_counter):05d}",
            "source": source,
            "grievances": "; ".join(sorted(comp)),
            "grievance_count": len(comp),
            "suppliers": "; ".join(all_sup),
            "plots": "; ".join(all_plots),
            "mills": "; ".join(all_mills),
            "issues": "; ".join(all_issues),
        })

events_df = pd.DataFrame(event_rows)
events_df.to_csv("/content/grievance_events_fixed.csv", index=False)

print("Done. Saved to /content/grievance_events_fixed.csv")
events_df.head(10)
