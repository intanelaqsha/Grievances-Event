import pandas as pd
import networkx as nx
from datetime import timedelta

# ====================================================
# 1. Load Data
# ====================================================
df = pd.read_csv("Grievances-Grid view-1.csv")

df['Date Filed'] = pd.to_datetime(df['Date Filed'], errors='coerce')

# Ensure list format for multi-value columns
for col in ['Suppliers', 'Mills', 'PIOConcessions', 'Source', 'Issues']:
    df[col] = df[col].fillna("").astype(str).apply(
        lambda x: [i.strip() for i in x.split(";") if i.strip()]
    )

# ====================================================
# 2. Issue grouping
# ====================================================
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
    for grp, items in ISSUE_GROUPS.items():
        if issue in items:
            return grp
    return None

df["IssueGroups"] = df["Issues"].apply(
    lambda issues: list(set([classify_issue(i) for i in issues if classify_issue(i)]))
)

# ====================================================
# 3. EXPAND → 1 ROW PER ISSUE-GROUP
# ====================================================
expanded = []

for idx, row in df.iterrows():
    for grp in row["IssueGroups"]:
        new_row = row.copy()
        new_row["orig_index"] = idx    # track original grievance
        new_row["IssueGroup"] = grp    # single group per row
        expanded.append(new_row)

exp_df = pd.DataFrame(expanded).reset_index(drop=True)

# ====================================================
# 4. BUILD GRAPH
# ====================================================
G = nx.Graph()

for i in exp_df.index:
    G.add_node(i)

def overlap(a, b, col):
    return bool(set(a[col]) & set(b[col]))

def share_entity(a, b):
    return (
        overlap(a, b, 'Suppliers') or
        overlap(a, b, 'Mills') or
        overlap(a, b, 'PIOConcessions')
    )

def within_time(a, b, group):
    days = abs((a['Date Filed'] - b['Date Filed']).days)
    if group == "Environmental":
        return days <= 365
    return days <= 30

for i in exp_df.index:
    for j in range(i+1, len(exp_df)):
        a, b = exp_df.loc[i], exp_df.loc[j]

        # Only compare rows of SAME issue-group
        if a["IssueGroup"] != b["IssueGroup"]:
            continue

        if share_entity(a, b) and within_time(a, b, a["IssueGroup"]):
            G.add_edge(i, j)

# ====================================================
# 5. EXTRACT EVENTS (connected components)
# ====================================================
events = []
event_id = 1

for comp in nx.connected_components(G):
    comp_df = exp_df.loc[list(comp)]

    events.append({
        "event_id": f"EVT-{event_id}",
        "IssueGroup": comp_df["IssueGroup"].iloc[0],
        "Suppliers": "; ".join(sorted(set(sum(comp_df['Suppliers'], [])))),
        "Mills": "; ".join(sorted(set(sum(comp_df['Mills'], [])))),
        "PIOConcessions": "; ".join(sorted(set(sum(comp_df['PIOConcessions'], [])))),
        "Source": "; ".join(sorted(set(sum(comp_df['Source'], [])))),
        "Issues": "; ".join(sorted(set(sum(sum(comp_df['Issues'], []), []))),
        "GrievanceCount": len(set(comp_df["orig_index"])),  # count unique grievances
    })

    event_id += 1

result = pd.DataFrame(events)
result.to_csv("grievance_events_graph_v2.csv", index=False)

print("Done → grievance_events_graph_v2.csv created")
result.head(20)
