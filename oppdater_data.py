"""
oppdater_data.py

Kjør dette scriptet lokalt for å oppdatere CSV-filene fra ClickHouse.
Etter kjøring, commit og push endringene til GitHub.

Bruk:
    python oppdater_data.py
"""

import clickhouse_connect

# Koble til ClickHouse
client = clickhouse_connect.get_client(host='localhost', port=8123, database='tromso_indikatorer')

# Eksport av kødata
print("Eksporterer kødata...")
df_ko = client.query_df(
    "SELECT dato, klokkeslett, stop_name, tid_dag, faktisk_tid, avstand, normal_tid, ko_min_km, forsinkelser, bil FROM `3-05 til dashbord ko`"
)
df_ko.to_csv("data/inndata_asker_ko.csv", sep=";", decimal=",", index=False, encoding="utf-8-sig")
print(f"Eksportert {len(df_ko)} rader (kødata)")

# Eksport av reisestatistikk
print("Eksporterer reisestatistikk...")
df_reiser = client.query_df(
    "SELECT ID, kvartal, bil, buss, sykkel, gange FROM `3-05 til dashbord reiser`"
)
df_reiser.to_csv("data/inndata_asker_reiser.csv", sep=";", decimal=",", index=False, encoding="utf-8-sig")
print(f"Eksportert {len(df_reiser)} rader (reisestatistikk)")

# Eksport av reisestatistikk
print("Eksporterer nøkkeltall...")
df_nokkel = client.query_df(
    "SELECT * FROM `3-06 Nokkeltall`"
)
df_nokkel.to_csv("data/inndata_asker_nokkel.csv", sep=";", decimal=",", index=False, encoding="utf-8-sig")
print(f"Eksportert {len(df_nokkel)} rader (Nøkkeltall)")

print("\nFerdig! Husk å committe og pushe til GitHub:")
print("  git add data/")
print("  git commit -m 'Oppdatert data'")
print("  git push")