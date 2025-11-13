# Corrected hierarchical grouping: include supplier edges (per source) + plot + mill
import pandas as pd
import re
import os
from collections import defaultdict

# -------- CONFIG ----------
IN_PATH = "/content/Grievances-Grid view-1.csv"
OUT_PATH = "/content/grievances_grouped_by_source_corrected.csv"
# --------------------------

if not os.path.exists(IN_PATH):
    raise FileNotFoundError(f"File not found: {IN_PATH}. Upload CSV ke path tersebut lalu jalankan ulang.")

# Helpers
def split_items(cell):
    """Split by comma/semicolon, trim and ignore empty."""
    if pd.isna(cell):
        return []
    s = str(cell).strip()
    if s == "":
        return []
    parts = re.split(r'[;,]\s*', s)
    return [p.strip() for p in parts if p.strip()]

def split_sources_sql_style(s):
    """Replicate SQL trick: preserve ', ' inside source names by placeholder,
       then split on comma (so 'A, B, C' => ['A', 'B', 'C'] while 'Name, Inc' preserved).
       Use only if you need it; otherwise fallback to split_items."""
    if pd.isna(s) or str(s).strip() == "":
        return []
    placeholder = "###SPACE_COMMA###"
    tmp = str(s).replace(", ", placeholder)
    parts = [p.strip() for p in tmp.split(",") if p.strip() != ""]
    parts = [p.replace(placeholder, ", ") for p in parts]
    return parts

# Read CSV
df = pd.read_csv(IN_PATH, dtype=str)

# Normalize column names: try to map the common variants to canonical names used below
cols = {c.lower(): c for c in df.columns}
def pick(colname_variants):
    for v in colname_variants:
        if v.lower() in cols:
            return cols[v.lower()]
    return None

col_id = pick(['id', 'ID']) or df.columns[0]
col_suppliers = pick(['suppliers', 'supplier']) or 'Suppliers'
col_plots = pick(['pioconcessions', 'combined plots', 'plots', 'plot_ids']) or 'PIOConcessions'
col_mills = pick(['mills', 'combined mills']) or 'Mills'
col_issues = pick(['issues','issue']) or 'Issues'
col_source = pick(['source','combined source','sources']) or 'Source'

# ensure columns exist
for c in [col_id, col_suppliers, col_plots, col_mills, col_issues, col_source]:
    if c not in df.columns:
        df[c] = ""

# Normalize columns to canonical local names
df = df.rename(columns={
    col_id: 'ID',
    col_suppliers: 'Suppliers',
    col_plots: 'PIOConcessions',
    col_mills: 'Mills',
    col_issues: 'Issues',
    col_source: 'Source'
})

# Expand rows by source (one row per source token)
rows = []
for _, r in df.iterrows():
    sources = split_sources_sql_style(r['Source'])
    if not sources:
        sources = ['']
    for s in sources:
        row = r.to_dict()
        row['source_expanded'] = s.strip()
        rows.append(row)
exp = pd.DataFrame(rows)

# Parse lists
exp['suppliers_list'] = exp['Suppliers'].apply(split_items)
exp['plots_list'] = exp['PIOConcessions'].apply(split_items)
exp['mills_list'] = exp['Mills'].apply(split_items)
exp['issues_list'] = exp['Issues'].apply(split_items)

# Union-Find implementation
def uf_make(nodes):
    parent = {n: n for n in nodes}
    rank = {n: 0 for n in nodes}
    return parent, rank

def uf_find(parent, x):
    if parent[x] != x:
        parent[x] = uf_find(parent, parent[x])
    return parent[x]

def uf_union(parent, rank, x, y):
    rx = uf_find(parent, x)
    ry = uf_find(parent, y)
    if rx == ry:
        return
    if rank[rx] < rank[ry]:
        parent[rx] = ry
    else:
        parent[ry] = rx
        if rank[rx] == rank[ry]:
            rank[rx] += 1

# Build groups per source
all_group_rows = []
mhg_counter = 1

for source_name, g in exp.groupby('source_expanded', sort=False):
    g = g.reset_index(drop=True)
    ids = g['ID'].astype(str).tolist()
    if not ids:
        continue

    parent, rank = uf_make(ids)

    # helper to union all pairs in a mapping (entity -> [ids])
    def union_pairs(mapping):
        for entity, idlist in mapping.items():
            idset = list(dict.fromkeys(idlist))  # preserve unique while maintaining order
            if len(idset) <= 1:
                continue
            # union every pair (chain unions is sufficient)
            first = idset[0]
            for other in idset[1:]:
                uf_union(parent, rank, first, other)

    # Build mappings within this source
    supplier_map = defaultdict(list)
    plot_map = defaultdict(list)
    mill_map = defaultdict(list)

    for _, row in g.iterrows():
        idv = str(row['ID'])
        for s in row['suppliers_list']:
            if s:
                supplier_map[s].append(idv)
        for p in row['plots_list']:
            if p:
                plot_map[p].append(idv)
        for m in row['mills_list']:
            if m:
                mill_map[m].append(idv)

    # Union IDs that share the same supplier (important fix)
    union_pairs(supplier_map)
    # Union IDs that share the same plot
    union_pairs(plot_map)
    # Union IDs that share the same mill
    union_pairs(mill_map)

    # Build components: root -> list(ids)
    comps = defaultdict(list)
    for idv in ids:
        root = uf_find(parent, idv)
        comps[root].append(idv)

    # For each component create aggregated row
    for comp_root, comp_ids in comps.items():
        comp_set = set(comp_ids)
        subset = g[g['ID'].isin(comp_set)]

        combined_suppliers = sorted({it for lst in subset['suppliers_list'] for it in lst if it})
        combined_plots = sorted({it for lst in subset['plots_list'] for it in lst if it})
        combined_mills = sorted({it for lst in subset['mills_list'] for it in lst if it})
        combined_issues = sorted({it for lst in subset['issues_list'] for it in lst if it})
        combined_sources = sorted({source_name})  # all same by design

        all_group_rows.append({
            'MHGID': f"MHG{str(mhg_counter).zfill(4)}",
            'grievance_IDs': '; '.join(sorted(comp_ids)),
            '#_grievance_count': len(comp_ids),
            'combined_suppliers': '; '.join(combined_suppliers),
            'combined_plots': '; '.join(combined_plots),
            'combined_mills': '; '.join(combined_mills),
            'combined_issues': '; '.join(combined_issues),
            'combined_source': '; '.join(combined_sources)
        })
        mhg_counter += 1

# Save results
out_df = pd.DataFrame(all_group_rows, columns=[
    'MHGID','grievance_IDs','#_grievance_count','combined_suppliers',
    'combined_plots','combined_mills','combined_issues','combined_source'
])
out_df.to_csv(OUT_PATH, index=False)
print(f"Saved {len(out_df)} groups to {OUT_PATH}")
out_df.head(20)
