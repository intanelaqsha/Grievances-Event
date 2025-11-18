#STEP 2 - Merge with the same Entity if same source
df2 = df_expanded.copy()

# Helper: unique sorted list
def uniq_list(x):
    return sorted(list(set(x)))

events = []
event_id = 1

# ---- CLUSTER PER SOURCE ----
for source, group in df2.groupby("Source"):
    group = group.reset_index(drop=True)

    source_events = []

    for idx, row in group.iterrows():

        # Ambil entitas
        sup = set(row["Suppliers"])
        mil = set(row["Mills"])
        pio = set(row["PIOConcessions"])
        iss = set(row["Issues"])

        # IMPORTANT → Grievance ID asli!
        gid = row["ID"]   # contoh: "Wilmar 1", "Bunge 2", dll

        merged = False

        # Cek overlap
        for evt in source_events:

            overlap = (
                len(sup & set(evt["Suppliers"])) > 0 or
                len(mil & set(evt["Mills"])) > 0 or
                len(pio & set(evt["PIOConcessions"])) > 0
            )

            if overlap:
                # Merge entitas
                evt["Suppliers"] = uniq_list(list(set(evt["Suppliers"]) | sup))
                evt["Mills"] = uniq_list(list(set(evt["Mills"]) | mil))
                evt["PIOConcessions"] = uniq_list(list(set(evt["PIOConcessions"]) | pio))
                evt["Issues"] = uniq_list(list(set(evt["Issues"]) | iss))

                # Merge grievance
                evt["Grievance_List"].append(gid)
                evt["Grievance_List"] = uniq_list(evt["Grievance_List"])
                evt["Grievance_Count"] = len(evt["Grievance_List"])

                merged = True
                break

        # Tidak overlap → buat event baru
        if not merged:
            source_events.append({
                "Event_ID": f"EVT_{event_id}",
                "Source": source,
                "Suppliers": uniq_list(list(sup)),
                "Mills": uniq_list(list(mil)),
                "PIOConcessions": uniq_list(list(pio)),
                "Issues": uniq_list(list(iss)),
                "Grievance_List": [gid],       # <── INI yang benar
                "Grievance_Count": 1
            })
            event_id += 1

    events.extend(source_events)

# Convert ke DataFrame
df_step2 = pd.DataFrame(events)

# List → string
for col in ["Suppliers", "Mills", "PIOConcessions", "Issues", "Grievance_List"]:
    df_step2[col] = df_step2[col].apply(lambda x: ", ".join(uniq_list(x)))

df_step2.head(20)
df_step2.to_csv('Step2.csv')
len(df_step2)
