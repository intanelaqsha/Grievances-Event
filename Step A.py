import pandas as pd
from itertools import count
from collections import defaultdict

# ===========================
# Disjoint Set (Union-Find)
# ===========================
class DSU:
    def __init__(self, n):
        self.parent = list(range(n))
        self.rank = [0]*n
    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x
    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb: return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        else:
            self.parent[rb] = ra
            if self.rank[ra] == self.rank[rb]:
                self.rank[ra] += 1

# ===========================
# LOAD
# ===========================
df = pd.read_csv("/content/Grievances-Grid view-1.csv", dtype=str)
df.columns = [c.strip() for c in df.columns]

# ===========================
# Robust Splitter (no bracket, semicolon only)
# ===========================
def split_entities(cell):
    """Untuk Suppliers, Mills, Plots, Issues:
       - hapus bracket
       - split hanya pada
       - unique
    """
    if pd.isna(cell) or str(cell).strip()=="":
        return []
    s = str(cell).replace("[","").replace("]","").strip()
    if "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
    else:
        parts = [s] if s else []
    return list(dict.fromkeys(parts))

def split_source(cell):
    """Untuk Source:
       - hilangkan bracket
       - split hanya pada ,
       - tanda lain dianggap bagian dari judul → TIDAK SPLIT
    """
    if pd.isna(cell) or str(cell).strip()=="":
        return []
    s = str(cell).replace("[","").replace("]","").strip()
    if ";" in s:
        parts = [p.strip() for p in s.split(";") if p.strip()]
    else:
        parts = [s] if s else []
    return list(dict.fromkeys(parts))

# prepare cols
for col in ["Suppliers","Mills","PIOConcessions","Source","Issues","ID","Date Filed"]:
    if col not in df.columns:
        df[col] = ""

df["Suppliers"] = df["Suppliers"].apply(split_entities)
df["Mills"] = df["Mills"].apply(split_entities)
df["PIOConcessions"] = df["PIOConcessions"].apply(split_entities)
df["Issues"] = df["Issues"].apply(split_entities)
df["Source"] = df["Source"].apply(split_source)

# ===========================
# EXPAND PER SOURCE
# ===========================
expanded = []
for _, r in df.iterrows():
    sources = r["Source"] if r["Source"] else ["NO_SOURCE"]
    for s in sources:
        expanded.append({
            "grievance": r["ID"],
            "suppliers": r["Suppliers"],
            "mills": r["Mills"],
            "plots": r["PIOConcessions"],
            "issues": r["Issues"],
            "source": s,
            "date": r["Date Filed"]
        })
exp = pd.DataFrame(expanded).reset_index(drop=True)
N = len(exp)

# ===========================
# UNION-FIND LOGIC
# ===========================
dsu = DSU(N)

sup_sets = [set(x) for x in exp["suppliers"]]
mill_sets = [set(x) for x in exp["mills"]]
plot_sets = [set(x) for x in exp["plots"]]
sources = exp["source"].tolist()
dates = exp["date"].tolist()

def has_overlap(i,j):
    return bool(
        sup_sets[i] & sup_sets[j] or
        mill_sets[i] & mill_sets[j] or
        plot_sets[i] & plot_sets[j]
    )

# Build inverted index
idx_by_supplier = defaultdict(set)
idx_by_mill = defaultdict(set)
idx_by_plot = defaultdict(set)

for i in range(N):
    for s in sup_sets[i]:
        idx_by_supplier[s].add(i)
    for m in mill_sets[i]:
        idx_by_mill[m].add(i)
    for p in plot_sets[i]:
        idx_by_plot[p].add(i)

for i in range(N):
    cand = set()
    for s in sup_sets[i]:
        cand |= idx_by_supplier[s]
    for m in mill_sets[i]:
        cand |= idx_by_mill[m]
    for p in plot_sets[i]:
        cand |= idx_by_plot[p]

    for j in cand:
        if j <= i:
            continue
        # rule 1: same source + overlap
        if sources[i] == sources[j] and has_overlap(i,j):
            dsu.union(i,j)
        # rule 2: same date + overlap (cross source)
        elif dates[i] == dates[j] and has_overlap(i,j):
            dsu.union(i,j)

# ===========================
# COMPONENTS → EVENTS
# ===========================
comp = defaultdict(list)
for i in range(N):
    comp[dsu.find(i)].append(i)

events = []
counter = 1

def uniq_join(values):
    """unique + sorted + join semicolon"""
    clean = [v for v in values if v and str(v).strip()]
    return "; ".join(sorted(dict.fromkeys(clean)))

for root, members in comp.items():
    suppliers_all = set()
    mills_all = set()
    plots_all = set()
    issues_all = set()
    sources_all = []
    grievances_all = []
    dates_seen = set()

    for i in members:
        suppliers_all.update(exp.at[i,"suppliers"])
        mills_all.update(exp.at[i,"mills"])
        plots_all.update(exp.at[i,"plots"])
        issues_all.update(exp.at[i,"issues"])
        sources_all.append(exp.at[i,"source"])
        grievances_all.append(exp.at[i,"grievance"])
        dates_seen.add(str(exp.at[i,"date"]))

    # unique + preserve order
    final_sources = list(dict.fromkeys(sources_all))

    events.append({
        "event_id": f"MHID{counter:03d}",
        "suppliers": uniq_join(suppliers_all),
        "mills": uniq_join(mills_all),
        "plots": uniq_join(plots_all),
        "issues": uniq_join(issues_all),
        "sources": "; ".join(final_sources),  # no brackets, unique
        "grievances": uniq_join(grievances_all),
        "grievance_count": len(set(grievances_all)),
        "date_filed": "; ".join(sorted(dates_seen))
    })

    counter += 1

final_df = pd.DataFrame(events)

# SAVE
final_df.to_csv("/content/StepA_revised.csv", index=False)
print("Saved /content/StepA_revised.csv")
final_df.head(10)
