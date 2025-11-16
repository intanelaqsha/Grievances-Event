import pandas as pd
import networkx as nx
from datetime import timedelta

# =====================================================
# 1. LOAD DATA
# =====================================================
df = pd.read_csv("Grievances-Grid view.csv")
df["Date Filed"] = pd.to_datetime(df["Date Filed"], errors="coerce")

# Convert list-like columns
for col in ["Suppliers", "Mills", "PIOConcessions", "Sources", "Issues"]:
    df[col] = df[col].fillna("").astype(str).apply(
        lambda x: [i.strip() for i in x.split(";") if i.strip()]
    )

# =====================================================
# 2. ISSUE GROUPING
# =====================================================
ISSUE_GROUPS = {
    "Environmental": [
        "Deforestation", "Peatland Loss", "Fires",
        "Riparian Issues", "Biodiversity loss"
    ],
    "Social": [
        "Labor Rights Violations", "Child Labor",
        "Violence and/or Coercion", "Gender and Ethnich Disparities",
        "Human Rights Violation", "Labor Disputes",
        "Wage Dispute", "Forced Labor and/or Child Labor"
    ],
    "Land Conflict": [
        "Land Disputes", "Land Grabbing",
        "Indigenous Peoples Conflict", "Limited Access to Services"
    ],
    "Governance": [
        "Corruption", "Illegal Infrastructure", "Infrastructure Damage"
    ]
}

def classify_issue(issue):
    for k, v in ISSUE_GROUPS.items():
        if issue in v:
            return k
    return None

df["IssueGroups"] = df["Issues"].apply(
    lambda issues: list({classify_issue(i) for i in issues if classify_issue(i)})
)

# =====================================================
# 3. SPLIT EACH GRIEVANCE INTO MULTIPLE NODES
# One node per (row, issueGroup)
# =====================================================
expanded = []

for idx, row in df.iterrows():
    for ig in row["IssueGroups"]:
        new_row = row.copy()
        new_row["IssueGroup"] = ig
        new_row["NodeID"] = f"{idx}-{ig}"
        expanded.append(new_row)

X = pd.DataFrame(expanded).set_index("NodeID")

# =====================================================
# 4. BUILD GRAPH
# =====================================================
G = nx.Graph()

for nid in X.index:
    G.add_node(nid)

def share_entity(a, b):
    return (
        bool(set(a["Suppliers"]) & set(b["Suppliers"])) or
        bool(set(a["Mills"]) & set(b["Mills"])) or
        bool(set(a["PIOConcessions"]) & set(b["PIOConcessions"]))
    )

def share_source(a, b):
    return bool(set(a["Sources"]) & set(b["Sources"]))

def within_window(a, b):
    days = abs((a["Date Filed"] - b["Date Filed"]).days)
    if a["IssueGroup"] == "Environmental" and b["IssueGroup"] == "Environmental":
        return days <= 365
    return days <= 30

# Pairwise edge building
for i in X.index:
    for j in X.index:
        if i >= j:
            continue
        a, b = X.loc[i], X.loc[j]

        if a["IssueGroup"] != b["IssueGroup"]:
            continue

        if (share_entity(a, b) or share_source(a, b)) and within_window(a, b):
            G.add_edge(i, j)

# =====================================================
# 5. EXTRACT EVENT CLUSTERS
# =====================================================
events = []
event_id = 1

for comp in nx.connected_components(G):
    comp_df = X.loc[list(comp)]

    # grievance counts per supplier group
    grievance_counts = {}
    for original_row in comp_df.index:
        base_id = original_row.split("-")[0]
        supplier_list = X.loc[original_row]["Suppliers"]
        for s in supplier_list:
            grievance_counts[s] = grievance_counts.get(s, 0) + 1

    events.append({
        "event_id": f"EVT-{event_id}",
        "IssueGroup": list(comp_df["IssueGroup"])[0],
        "Suppliers": "; ".join(sorted(set(sum(comp_df["Suppliers"], [])))),
        "Mills": "; ".join(sorted(set(sum(comp_df["Mills"], [])))),
        "PIOConcessions": "; ".join(sorted(set(sum(comp_df["PIOConcessions"], [])))),
        "Sources": "; ".join(sorted(set(sum(comp_df["Sources"], [])))),
        "Issues": "; ".join(sorted(set(sum(comp_df["Issues"], [])))),
        "GrievancesCount": len(set([i.split("-")[0] for i in comp_df.index])),
        "GrievancesBreakdown": "; ".join([f"{k}: {v}" for k, v in grievance_counts.items()])
    })
    event_id += 1

res = pd.DataFrame(events)
res.to_csv("grievance_events_graph_v3.csv", index=False)

print("Selesai → grievance_events_graph_v3.csv dibuat")
res.head(15)
