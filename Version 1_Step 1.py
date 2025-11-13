# --- Grievance Event Grouping Script ---
import pandas as pd
from collections import defaultdict
import re
from itertools import count
import os

# === PATH FILE ===
file_path = "/content/Grievances-Grid view-1.csv"  


if not os.path.exists(file_path):
    raise FileNotFoundError(f"⚠️ File {file_path} tidak ditemukan. Pastikan sudah diupload ke Colab.")

# === Fungsi bantu ===
def split_items(cell):
    """Pisahkan isi cell dengan pemisah koma/semicolon dan bersihkan spasi."""
    if pd.isna(cell):
        return []
    s = str(cell).strip()
    if s == "":
        return []
    parts = re.split(r'[;,]\s*', s)
    return [p.strip() for p in parts if p.strip() != '']

def normalize_name(n):
    """Normalisasi nama entitas (hapus spasi berlebih)."""
    return re.sub(r'\s+', ' ', str(n).strip()) if pd.notna(n) else ''

# === Load CSV ===
df = pd.read_csv(file_path, dtype=str)
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

# Pastikan kolom yang dibutuhkan ada
for col in ['id', 'suppliers', 'source', 'mills', 'pioconcessions', 'issues']:
    if col not in df.columns:
        df[col] = ''

# === Expand sources (jika 1 grievance punya banyak source) ===
rows = []
for _, r in df.iterrows():
    sources = split_items(r['source'])
    if len(sources) == 0:
        sources = ['']
    for s in sources:
        row = r.to_dict()
        row['source_expanded'] = normalize_name(s)
        rows.append(row)

exp = pd.DataFrame(rows)
exp['suppliers_list'] = exp['suppliers'].apply(lambda x: [normalize_name(i) for i in split_items(x)])
exp['plots_list'] = exp['pioconcessions'].apply(lambda x: [normalize_name(i) for i in split_items(x)])
exp['mills_list'] = exp['mills'].apply(lambda x: [normalize_name(i) for i in split_items(x)])
exp['issues_list'] = exp['issues'].apply(lambda x: [normalize_name(i) for i in split_items(x)])

# === Mapping ID → isu unik ===
id_to_issues = exp.groupby('id')['issues_list'].agg(lambda lists: list({it for sub in lists for it in sub})).to_dict()

# === Build Events ===
event_rows = []
evt_counter = count(1)

for source, group in exp.groupby('source_expanded', sort=False):
    # --- SUPPLIER LEVEL ---
    supplier_to_ids = defaultdict(set)
    for _, r in group.iterrows():
        for sup in r['suppliers_list']:
            if sup:
                supplier_to_ids[sup].add(r['id'])

    for sup, ids in supplier_to_ids.items():
        issues = set()
        for gid in ids:
            issues.update(id_to_issues.get(gid, []))
        event_rows.append({
            'event_id': f"EVT-{next(evt_counter):05d}",
            'level': 'supplier',
            'entity': sup,
            'source': source,
            'grievances': '; '.join(sorted(ids)),
            'grievance_count': len(ids),
            'issues': '; '.join(sorted(issues))
        })

    # --- PLOT LEVEL ---
    plot_to_ids = defaultdict(set)
    for _, r in group.iterrows():
        for p in r['plots_list']:
            if p:
                plot_to_ids[p].add(r['id'])

    for p, ids in plot_to_ids.items():
        issues = set()
        for gid in ids:
            issues.update(id_to_issues.get(gid, []))
        event_rows.append({
            'event_id': f"EVT-{next(evt_counter):05d}",
            'level': 'plot',
            'entity': p,
            'source': source,
            'grievances': '; '.join(sorted(ids)),
            'grievance_count': len(ids),
            'issues': '; '.join(sorted(issues))
        })

    # --- MILL LEVEL ---
    mill_to_ids = defaultdict(set)
    for _, r in group.iterrows():
        for m in r['mills_list']:
            if m:
                mill_to_ids[m].add(r['id'])

    for m, ids in mill_to_ids.items():
        issues = set()
        for gid in ids:
            issues.update(id_to_issues.get(gid, []))
        event_rows.append({
            'event_id': f"EVT-{next(evt_counter):05d}",
            'level': 'mill',
            'entity': m,
            'source': source,
            'grievances': '; '.join(sorted(ids)),
            'grievance_count': len(ids),
            'issues': '; '.join(sorted(issues))
        })

# === Simpan hasil ===
events_df = pd.DataFrame(event_rows, columns=['event_id','level','entity','source','grievances','grievance_count','issues'])
out_path = "/content/grievance_events_final.csv"
events_df.to_csv(out_path, index=False)

print(f"✅ Selesai! Total {len(events_df)} events tersimpan di: {out_path}\n")
display(events_df.head(10))
