"""
generer_dashboard.py

Genererer en statisk HTML-fil med interaktive Plotly-grafer og JavaScript-filtre.
HTML-filen kan hostes på GitHub Pages.

Bruk:
    python generer_dashboard.py

Output:
    docs/index.html (legg denne i docs/ for GitHub Pages)
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime


def load_and_process_ko_data(filepath):
    """Last inn og preprosesser kødata"""
    df = pd.read_csv(filepath, sep=";", decimal=",", encoding="utf-8-sig")
    df.columns = df.columns.str.strip().str.replace('\ufeff', '')
    df.columns = df.columns.str.lower()

    df["dato"] = pd.to_datetime(df["dato"])
    df["dato_str"] = df["dato"].dt.strftime("%d.%m.%Y")

    df["forsinkelser"] = pd.to_numeric(df["forsinkelser"], errors="coerce")
    df["ko_min_km"] = pd.to_numeric(df["ko_min_km"], errors="coerce")
    df["bil"] = pd.to_numeric(df["bil"], errors="coerce")

    return df


def load_and_process_reiser_data(filepath):
    """Last inn og preprosesser reisedata"""
    df = pd.read_csv(filepath, sep=";", decimal=",", encoding="utf-8-sig")
    df.columns = df.columns.str.strip().str.replace('\ufeff', '')

    for col in ["bil", "buss", "sykkel", "gange"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["kvartal_sort"] = df["kvartal"].str.replace("-", "").astype(int)
    df = df.sort_values("kvartal_sort").reset_index(drop=True)

    return df


def load_and_process_nokkel_data(filepath):
    """Last inn og preprosesser nøkkeltalldata"""
    df = pd.read_csv(filepath, sep=";", decimal=",", encoding="utf-8-sig")
    df.columns = df.columns.str.strip().str.replace('\ufeff', '')

    # Behandle delområder som string
    df["delomrade_fra"] = df["delomrade_fra"].astype(str).str.strip()
    df["delomrade_til"] = df["delomrade_til"].astype(str).str.strip()

    df["reiser"] = pd.to_numeric(df["reiser"], errors="coerce")
    df["co2_tonn"] = pd.to_numeric(df["co2_tonn"], errors="coerce")

    # Lag sorteringsnøkkel for kvartal
    df["kvartal_sort"] = df["kvartal"].str.replace("-", "").astype(int)

    return df


def aggregate_ko_data(df):
    """Aggreger kødata for grafer"""
    aggregated = {}

    for tid_dag in ["Morgen", "Ettermiddag"]:
        df_tid = df[df["tid_dag"] == tid_dag].copy()

        if len(df_tid) == 0:
            continue

        def weighted_avg_ko(group):
            mask = group["ko_min_km"].notna() & group["bil"].notna() & (group["bil"] > 0)
            if mask.sum() == 0:
                return np.nan
            return (group.loc[mask, "ko_min_km"] * group.loc[mask, "bil"]).sum() / group.loc[mask, "bil"].sum()

        def weighted_avg_forsinkelser(group):
            mask = group["forsinkelser"].notna() & group["bil"].notna() & (group["bil"] > 0)
            if mask.sum() == 0:
                return np.nan
            return (group.loc[mask, "forsinkelser"] * group.loc[mask, "bil"]).sum() / group.loc[mask, "bil"].sum()

        agg_alle_dato = df_tid.groupby("dato").apply(
            lambda g: pd.Series({
                "ko_min_km": weighted_avg_ko(g),
                "forsinkelser": weighted_avg_forsinkelser(g)
            }), include_groups=False
        ).reset_index()
        agg_alle_dato = agg_alle_dato.sort_values("dato")
        agg_alle_dato["dato_str"] = agg_alle_dato["dato"].dt.strftime("%d.%m.%Y")
        agg_alle_dato["dato_iso"] = agg_alle_dato["dato"].dt.strftime("%Y-%m-%d")

        key = f"Alle strekninger_{tid_dag}"
        aggregated[key] = {
            "datoer": agg_alle_dato["dato_str"].tolist(),
            "datoer_iso": agg_alle_dato["dato_iso"].tolist(),
            "ko": [round(x, 3) if pd.notna(x) else None for x in agg_alle_dato["ko_min_km"].tolist()],
            "forsinkelser": [round(x, 3) if pd.notna(x) else None for x in agg_alle_dato["forsinkelser"].tolist()]
        }

        # Klokkeslett-data med dato for filtrering (rådata per dato og klokkeslett)
        agg_alle_klokke_dato = df_tid.groupby(["dato", "klokkeslett"]).apply(
            lambda g: pd.Series({
                "ko_min_km": weighted_avg_ko(g),
                "forsinkelser": weighted_avg_forsinkelser(g)
            }), include_groups=False
        ).reset_index()
        agg_alle_klokke_dato["dato_iso"] = agg_alle_klokke_dato["dato"].dt.strftime("%Y-%m-%d")

        key = f"Alle strekninger_{tid_dag}_klokkeslett_raw"
        aggregated[key] = {
            "records": [
                {
                    "dato_iso": row["dato_iso"],
                    "klokkeslett": row["klokkeslett"],
                    "ko": round(row["ko_min_km"], 3) if pd.notna(row["ko_min_km"]) else None,
                    "forsinkelser": round(row["forsinkelser"], 3) if pd.notna(row["forsinkelser"]) else None
                }
                for _, row in agg_alle_klokke_dato.iterrows()
            ]
        }

        # Beholder også pre-aggregert for bakoverkompatibilitet
        agg_alle_klokke = df_tid.groupby("klokkeslett").apply(
            lambda g: pd.Series({
                "ko_min_km": weighted_avg_ko(g),
                "forsinkelser": weighted_avg_forsinkelser(g)
            }), include_groups=False
        ).reset_index()
        agg_alle_klokke = agg_alle_klokke.sort_values("klokkeslett")

        key = f"Alle strekninger_{tid_dag}_klokkeslett"
        aggregated[key] = {
            "klokkeslett": agg_alle_klokke["klokkeslett"].tolist(),
            "ko": [round(x, 3) if pd.notna(x) else None for x in agg_alle_klokke["ko_min_km"].tolist()],
            "forsinkelser": [round(x, 3) if pd.notna(x) else None for x in agg_alle_klokke["forsinkelser"].tolist()]
        }

        for stop in df_tid["stop_name"].dropna().unique():
            df_stop = df_tid[df_tid["stop_name"] == stop]

            agg = df_stop.groupby(["dato", "dato_str"]).agg({
                "ko_min_km": "median",
                "forsinkelser": "median"
            }).reset_index()
            agg = agg.sort_values("dato")
            agg["dato_iso"] = agg["dato"].dt.strftime("%Y-%m-%d")

            key = f"{stop}_{tid_dag}"
            aggregated[key] = {
                "datoer": agg["dato_str"].tolist(),
                "datoer_iso": agg["dato_iso"].tolist(),
                "ko": [round(x, 3) if pd.notna(x) else None for x in agg["ko_min_km"].tolist()],
                "forsinkelser": [round(x, 3) if pd.notna(x) else None for x in agg["forsinkelser"].tolist()]
            }

            # Klokkeslett-data med dato for filtrering
            agg_klokke_dato = df_stop.groupby(["dato", "klokkeslett"]).agg({
                "ko_min_km": "median",
                "forsinkelser": "median"
            }).reset_index()
            agg_klokke_dato["dato_iso"] = agg_klokke_dato["dato"].dt.strftime("%Y-%m-%d")

            key = f"{stop}_{tid_dag}_klokkeslett_raw"
            aggregated[key] = {
                "records": [
                    {
                        "dato_iso": row["dato_iso"],
                        "klokkeslett": row["klokkeslett"],
                        "ko": round(row["ko_min_km"], 3) if pd.notna(row["ko_min_km"]) else None,
                        "forsinkelser": round(row["forsinkelser"], 3) if pd.notna(row["forsinkelser"]) else None
                    }
                    for _, row in agg_klokke_dato.iterrows()
                ]
            }

            agg_klokke = df_stop.groupby("klokkeslett").agg({
                "ko_min_km": "median",
                "forsinkelser": "median"
            }).reset_index()
            agg_klokke = agg_klokke.sort_values("klokkeslett")

            key = f"{stop}_{tid_dag}_klokkeslett"
            aggregated[key] = {
                "klokkeslett": agg_klokke["klokkeslett"].tolist(),
                "ko": [round(x, 3) if pd.notna(x) else None for x in agg_klokke["ko_min_km"].tolist()],
                "forsinkelser": [round(x, 3) if pd.notna(x) else None for x in agg_klokke["forsinkelser"].tolist()]
            }

    return aggregated


def calculate_first_dates(ko_aggregated):
    """Beregn første dato med data for kø og forsinkelser"""
    first_ko_date = None
    first_forsinkelser_date = None

    # Sjekk alle datasett for å finne første dato med faktisk data
    for key, data in ko_aggregated.items():
        if '_klokkeslett' in key or 'datoer_iso' not in data:
            continue

        datoer_iso = data['datoer_iso']
        ko_values = data['ko']
        forsinkelser_values = data['forsinkelser']

        # Finn første dato med kø-data
        for i, (dato, ko) in enumerate(zip(datoer_iso, ko_values)):
            if ko is not None:
                if first_ko_date is None or dato < first_ko_date:
                    first_ko_date = dato
                break

        # Finn første dato med forsinkelser-data
        for i, (dato, fors) in enumerate(zip(datoer_iso, forsinkelser_values)):
            if fors is not None:
                if first_forsinkelser_date is None or dato < first_forsinkelser_date:
                    first_forsinkelser_date = dato
                break

    return first_ko_date, first_forsinkelser_date


def prepare_nokkel_data(df):
    """Forbered nøkkeltalldata for JavaScript"""
    # Lag liste over unike verdier
    omrader_fra = sorted(df["delomrade_fra"].unique().tolist())
    omrader_til = sorted(df["delomrade_til"].unique().tolist())
    tider = sorted(df["time_of_day"].unique().tolist())
    kvartaler = df.sort_values("kvartal_sort")["kvartal"].unique().tolist()

    # Konverter hele datasettet til liste av dicts for JavaScript (uten trend - beregnes i JS)
    records = df[
        ["delomrade_fra", "delomrade_til", "kvartal", "reiser", "co2_tonn", "time_of_day",
         "weekday_indicator"]].to_dict(
        "records")

    return {
        "records": records,
        "omrader_fra": omrader_fra,
        "omrader_til": omrader_til,
        "tider": tider,
        "kvartaler": kvartaler
    }


def generate_html(ko_data, reiser_data, ko_aggregated, nokkel_data, first_ko_date, first_forsinkelser_date):
    """Generer HTML med embedded data og JavaScript"""

    strekninger_ko = ["Alle strekninger"] + sorted(ko_data["stop_name"].dropna().unique().tolist())
    strekninger_reiser = sorted(reiser_data["ID"].unique().tolist())

    # Forbered reisedata som dict
    reiser_dict = {}
    for strekning in strekninger_reiser:
        df_s = reiser_data[reiser_data["ID"] == strekning].sort_values("kvartal_sort")
        reiser_dict[strekning] = {
            "kvartaler": df_s["kvartal"].tolist(),
            "bil": [round(x, 2) if pd.notna(x) else None for x in df_s["bil"].tolist()],
            "buss": [round(x, 2) if pd.notna(x) else None for x in df_s["buss"].tolist()],
            "sykkel": [round(x, 2) if pd.notna(x) else None for x in df_s["sykkel"].tolist()],
            "gange": [round(x, 2) if pd.notna(x) else None for x in df_s["gange"].tolist()]
        }

    # Generer options for flervalg
    omrade_fra_options = '<option value="Alle" selected>Alle</option>\n' + \
                         "\n".join(f'<option value="{o}">{o}</option>' for o in nokkel_data["omrader_fra"])
    omrade_til_options = '<option value="Alle" selected>Alle</option>\n' + \
                         "\n".join(f'<option value="{o}">{o}</option>' for o in nokkel_data["omrader_til"])

    # Generer radiobuttons for tid på dagen
    tid_radios = '<label><input type="radio" name="tid-nokkel" value="Alle" checked onchange="updateNokkelChart()"> Alle</label>\n'
    for tid in sorted(nokkel_data["tider"]):
        tid_radios += f'<label><input type="radio" name="tid-nokkel" value="{tid}" onchange="updateNokkelChart()"> {tid}</label>\n'

    html = f'''<!DOCTYPE html>
<html lang="no">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Mobilitetsdashbord - Tromsø</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background-color: #f5f5f5;
        }}
        .header {{
            background-color: #2c5f7c;
            color: white;
            padding: 20px;
            text-align: center;
        }}
        .nav {{
            display: flex;
            gap: 0;
            background-color: #6b7b8c;
            padding: 0;
        }}
        .nav button {{
            background-color: #6b7b8c;
            color: white;
            border: none;
            padding: 15px 30px;
            cursor: pointer;
            font-size: 16px;
            transition: background-color 0.2s;
        }}
        .nav button:hover {{
            background-color: #5a6a7a;
        }}
        .nav button.active {{
            background-color: #4a5a6a;
        }}
        .container {{
            display: flex;
            min-height: calc(100vh - 120px);
        }}
        .sidebar {{
            width: 280px;
            background-color: #e8e8e8;
            padding: 20px;
            flex-shrink: 0;
        }}
        .sidebar h3 {{
            background-color: #2c5f7c;
            color: white;
            padding: 15px;
            margin: -20px -20px 20px -20px;
        }}
        .sidebar label {{
            display: block;
            margin-top: 15px;
            font-weight: bold;
        }}
        .sidebar select, .sidebar input {{
            width: 100%;
            padding: 8px;
            margin-top: 5px;
            border: 1px solid #ccc;
            border-radius: 4px;
        }}
        .sidebar select[multiple] {{
            height: 150px;
        }}
        .sidebar hr {{
            margin: 20px 0;
            border: none;
            border-top: 1px solid #ccc;
        }}
        .main {{
            flex: 1;
            padding: 30px;
        }}
        .page {{
            display: none;
        }}
        .page.active {{
            display: block;
        }}
        .chart {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .sankey-btn {{
            background-color: #2c5f7c;
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            margin-top: 10px;
        }}
        .sankey-btn:hover {{
            background-color: #1e4a5f;
        }}
        .chart-buttons {{
            display: flex;
            gap: 10px;
            margin-top: 10px;
        }}
        .modal {{
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0,0,0,0.5);
        }}
        .modal-content {{
            background-color: white;
            margin: 2% auto;
            padding: 20px;
            border-radius: 8px;
            width: 95%;
            max-width: 1100px;
            max-height: 90vh;
            overflow-y: auto;
        }}
        .modal-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid #eee;
        }}
        .modal-header h2 {{
            margin: 0;
            color: #2c5f7c;
        }}
        .modal-close {{
            font-size: 28px;
            cursor: pointer;
            color: #666;
        }}
        .modal-close:hover {{
            color: #333;
        }}
        .sankey-controls {{
            display: flex;
            gap: 20px;
            margin-bottom: 15px;
            align-items: center;
        }}
        .sankey-controls label {{
            display: flex;
            align-items: center;
            gap: 5px;
            cursor: pointer;
        }}
        .sankey-controls label.disabled {{
            display: none;
        }}
        .home-grid {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 20px;
        }}
        .home-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            cursor: pointer;
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        .home-card:hover {{
            transform: translateY(-3px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }}
        .home-card h3 {{
            margin-bottom: 15px;
            color: #2c5f7c;
        }}
        .kart-thumbnail {{
            width: 300px;
            border: 2px solid #2c5f7c;
            border-radius: 8px;
            cursor: pointer;
            transition: transform 0.2s;
        }}
        .kart-thumbnail:hover {{
            transform: scale(1.02);
        }}
        .kart-button {{
            display: inline-block;
            background-color: #2c5f7c;
            color: white;
            padding: 12px 24px;
            text-decoration: none;
            border-radius: 4px;
            margin-top: 15px;
        }}
        .kart-button:hover {{
            background-color: #1e4a5f;
        }}
        .radio-group {{
            margin-top: 10px;
        }}
        .radio-group label {{
            display: flex;
            align-items: center;
            font-weight: normal;
            margin-top: 8px;
            cursor: pointer;
        }}
        .radio-group input {{
            width: auto;
            margin-right: 8px;
        }}
        .filter-hint {{
            font-size: 12px;
            color: #666;
            margin-top: 5px;
            font-weight: normal;
        }}
        @media (max-width: 900px) {{
            .container {{
                flex-direction: column;
            }}
            .sidebar {{
                width: 100%;
            }}
            .home-grid {{
                grid-template-columns: 1fr;
            }}
        }}
        .help-content {{
            max-width: 800px;
            line-height: 1.6;
        }}
        .help-content h2 {{
            color: #2c5f7c;
            margin-bottom: 30px;
            padding-bottom: 10px;
            border-bottom: 2px solid #2c5f7c;
        }}
        .help-content section {{
            background: white;
            padding: 25px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .help-content h3 {{
            color: #2c5f7c;
            margin-bottom: 15px;
        }}
        .help-content ul, .help-content ol {{
            margin-left: 20px;
            margin-top: 10px;
            margin-bottom: 10px;
        }}
        .help-content li {{
            margin-bottom: 8px;
        }}
        .help-content p {{
            margin-bottom: 10px;
        }}
        .help-content em {{
            color: #666;
        }}
        .help-content a {{
            color: #2c5f7c;
        }}
        .help-content .contact-section {{
            background-color: #e8f4f8;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Mobilitetsdashbord for Tromsø</h1>
    </div>

    <div class="nav">
        <button class="active" onclick="showPage('hjem')">Hjem</button>
        <button onclick="showPage('forsinkelser')">Forsinkelser og køer</button>
        <button onclick="showPage('reisestatistikk')">Reisestatistikk Tromsø sentrum</button>
        <button onclick="showPage('nokkeltall')">Reisestrømmer i Tromsø kommune</button>
        <button onclick="showPage('kart')">Kart</button>
        <button onclick="showPage('hjelp')">Hjelp</button>
    </div>

    <div class="container">
        <div class="sidebar" id="sidebar-forsinkelser" style="display: none;">
            <h3>Velg filtre</h3>

            <label for="strekning-ko">Strekning</label>
            <select id="strekning-ko" multiple onchange="updateKoChart()">
                <option value="Alle strekninger" selected>Alle strekninger</option>
                {"".join(f'<option value="{s}">{s}</option>' for s in strekninger_ko if s != 'Alle strekninger')}
            </select>
            <div class="filter-hint">Ctrl+klikk for flervalg</div>

            <hr>

            <label>Velg visning:</label>
            <div class="radio-group">
                <label><input type="radio" name="visning" value="ko" checked onchange="onVisningChange()"> Kø</label>
                <label><input type="radio" name="visning" value="forsinkelser" onchange="onVisningChange()"> Forsinkelser buss</label>
            </div>

            <hr>

            <label>Vis over:</label>
            <div class="radio-group">
                <label><input type="radio" name="xakse" value="dato" checked onchange="updateKoChart()"> Over dato</label>
                <label><input type="radio" name="xakse" value="klokkeslett" onchange="updateKoChart()"> Over klokkeslett</label>
            </div>

            <hr>

            <label>Tid på døgnet:</label>
            <div class="radio-group">
                <label><input type="radio" name="tid" value="Morgen" checked onchange="updateKoChart()"> Morgen</label>
                <label><input type="radio" name="tid" value="Ettermiddag" onchange="updateKoChart()"> Ettermiddag</label>
            </div>

            <hr>

            <label for="startdato-ko">Fra dato</label>
            <input type="date" id="startdato-ko" onchange="updateKoChart()">
            <div class="filter-hint" id="startdato-hint"></div>
        </div>

        <div class="sidebar" id="sidebar-reisestatistikk" style="display: none;">
            <h3>Velg filtre</h3>

            <label for="strekning-reiser">Strekning</label>
            <select id="strekning-reiser" onchange="updateReiserChart()">
                {"".join(f'<option value="{s}"' + (' selected' if s == 'Til Tromsø sentrum' else '') + f'>{s}</option>' for s in strekninger_reiser)}
            </select>

            <hr>

            <label for="transportmiddel">Transportmiddel</label>
            <select id="transportmiddel" multiple onchange="updateReiserChart()">
                <option value="Alle" selected>Alle</option>
                <option value="bil">Bil</option>
                <option value="buss">Buss</option>
                <option value="sykkel">Sykkel</option>
                <option value="gange">Gange</option>
            </select>
            <div class="filter-hint">Ctrl+klikk for flervalg</div>
        </div>

        <div class="sidebar" id="sidebar-nokkeltall" style="display: none;">
            <h3>Velg filtre</h3>

            <label for="omrade-fra">Område fra</label>
            <select id="omrade-fra" multiple onchange="updateNokkelChart()">
                {omrade_fra_options}
            </select>
            <div class="filter-hint">Ctrl+klikk for flervalg</div>

            <label for="omrade-til">Område til</label>
            <select id="omrade-til" multiple onchange="updateNokkelChart()">
                {omrade_til_options}
            </select>
            <div class="filter-hint">Ctrl+klikk for flervalg</div>

            <hr>

            <label>Vis:</label>
            <div class="radio-group">
                <label><input type="radio" name="visning-nokkel" value="reiser" checked onchange="updateNokkelChart()"> Reiser</label>
                <label><input type="radio" name="visning-nokkel" value="co2_sum" onchange="updateNokkelChart()"> CO2-utslipp (sum)</label>
                <label><input type="radio" name="visning-nokkel" value="co2_per_reise" onchange="updateNokkelChart()"> CO2 per reise</label>
            </div>

            <hr>

            <label>Tid på dagen:</label>
            <div class="radio-group">
                {tid_radios}
            </div>

            <hr>

            <label>Ukedag/helg:</label>
            <div class="radio-group">
                <label><input type="radio" name="ukedag-nokkel" value="Alle" checked onchange="updateNokkelChart()"> Alle</label>
                <label><input type="radio" name="ukedag-nokkel" value="Weekday" onchange="updateNokkelChart()"> Ukedag</label>
                <label><input type="radio" name="ukedag-nokkel" value="Weekend" onchange="updateNokkelChart()"> Helg</label>
            </div>
        </div>

        <div class="main">
            <!-- HJEM -->
            <div class="page active" id="page-hjem">
                <h2>Velkommen til Mobilitetsdashbordet</h2>
                <p style="margin: 20px 0;">
                    Dette dashbordet gir en oversikt over sentrale mobilitetsindikatorer for Tromsø kommune.
                    Her kan du følge med på utviklingen i kø, forsinkelser og reisemønstre over tid.
                </p>

                <div class="home-grid">
                    <div class="home-card" onclick="navigateTo('forsinkelser')">
                        <h3>📊 Forsinkelser og køer</h3>
                        <p>Oversikt over kø og forsinkelser på utvalgte strekninger i Tromsø.</p>
                        <ul style="margin-top: 10px; margin-left: 20px;">
                            <li>Køindeks (min/km)</li>
                            <li>Forsinkelser for buss</li>
                            <li>Filtrer på tid og strekning</li>
                        </ul>
                    </div>
                    <div class="home-card" onclick="navigateTo('kart')">
                        <h3>🗺️ Kart</h3>
                        <p>Interaktivt kart over Tromsø sentrum.</p>
                        <ul style="margin-top: 10px; margin-left: 20px;">
                            <li>Trafikkmønstre til/fra sentrum</li>
                            <li>Gjennomfartstrafikk</li>
                            <li>Oversikt over køer</li>
                            <li>Soner og grids</li>
                        </ul>
                    </div>
                    <div class="home-card" onclick="navigateTo('reisestatistikk')">
                        <h3>🚌 Reisestatistikk Tromsø sentrum</h3>
                        <p>Statistikk over reiser og reisemønstre.</p>
                        <ul style="margin-top: 10px; margin-left: 20px;">
                            <li>Antall reiser per kvartal</li>
                            <li>Fordelt på transportmiddel</li>
                            <li>Filtrer på strekning</li>
                        </ul>
                    </div>
                    <div class="home-card" onclick="navigateTo('nokkeltall')">
                        <h3>📈 Reisestrømmer i Tromsø kommune</h3>
                        <p>Detaljert reisestatistikk mellom områder.</p>
                        <ul style="margin-top: 10px; margin-left: 20px;">
                            <li>Filtrer på fra/til-område</li>
                            <li>Tid på dagen</li>
                            <li>Ukedag/helg</li>
                        </ul>
                    </div>
                </div>
            </div>

            <!-- FORSINKELSER -->
            <div class="page" id="page-forsinkelser">
                <div class="chart">
                    <div id="ko-chart" style="height: 500px;"></div>
                </div>
            </div>

            <!-- REISESTATISTIKK -->
            <div class="page" id="page-reisestatistikk">
                <div class="chart">
                    <div id="reiser-chart" style="height: 500px;"></div>
                </div>
            </div>

            <!-- NØKKELTALL REISER -->
            <div class="page" id="page-nokkeltall">
                <div class="chart">
                    <div id="nokkel-chart" style="height: 500px;"></div>
                    <div class="chart-buttons">
                        <button id="sankey-btn" class="sankey-btn" onclick="openSankeyModal()" style="display: none;">📊 Vis reisestrømmer</button>
                        <button id="csv-btn" class="sankey-btn" onclick="exportCSV()">📥 Eksporter CSV</button>
                    </div>
                </div>
            </div>

            <!-- SANKEY MODAL -->
            <div id="sankey-modal" class="modal">
                <div class="modal-content">
                    <div class="modal-header">
                        <h2>Reisestrømmer</h2>
                        <span class="modal-close" onclick="closeSankeyModal()">&times;</span>
                    </div>
                    <div class="sankey-controls" id="sankey-controls">
                        <span><strong>Vis retning:</strong></span>
                        <label id="sankey-fra-label"><input type="radio" name="sankey-retning" value="fra" checked onchange="updateSankeyChart()"> Fra valgte områder</label>
                        <label id="sankey-til-label"><input type="radio" name="sankey-retning" value="til" onchange="updateSankeyChart()"> Til valgte områder</label>
                    </div>
                    <div id="sankey-chart" style="height: 600px;"></div>
                    <p style="color: #666; font-size: 12px; margin-top: 10px;">Viser topp 10 relasjoner basert på siste 4 kvartaler.</p>
                </div>
            </div>

            <!-- KART -->
            <div class="page" id="page-kart">
                <h2>Kart - Tromsø sentrum</h2>
                <p style="margin: 20px 0;">Interaktivt kart som viser trafikkmønstre i Tromsø sentrum.</p>

                <a href="https://qgiscloud.com/jaleas/Troms__Case/?l=Til%20Kvaløysletta%20Morgen%2CFra%20Kvaløysletta%20Ettermiddag!%2CTil%20Kroken%20Morgen!%2CFra%20Kroken%20Ettermiddag!%2C0-01%20case%5B52%5D%2CGrey&t=Troms__Case&e=2066340%2C10939113%2C2154182%2C10985892" target="_blank">
                    <img src="https://raw.githubusercontent.com/wdmkkd6ps4-cmd/Tromsø-dashbord/main/data/kart_thumbnail.jpg" class="kart-thumbnail" alt="Kart over Tromsø">
                </a>

                <br>

                <a href="https://qgiscloud.com/jaleas/Troms__Case/?l=Til%20Kvaløysletta%20Morgen%2CFra%20Kvaløysletta%20Ettermiddag!%2CTil%20Kroken%20Morgen!%2CFra%20Kroken%20Ettermiddag!%2C0-01%20case%5B52%5D%2CGrey&t=Troms__Case&e=2066340%2C10939113%2C2154182%2C10985892" target="_blank" class="kart-button">
                    🗺️ Åpne interaktivt kart i ny fane
                </a>

                <div style="margin-top: 30px;">
                    <h3>Tilgjengelige lag i kartet:</h3>
                    <ul style="margin-top: 10px; margin-left: 20px;">
                        <li>Til Tromsø sentrum (Morgen)</li>
                        <li>Fra Tromsø sentrum (Ettermiddag)</li>
                        <li>Gjennomfartstrafikk Syd-Nord</li>
                        <li>Gjennomfartstrafikk Syd-Vest</li>
                        <li>Oversiktskart over køer</li>
                        <li>Soner og områdeinndeling</li>
                    </ul>
                </div>
            </div>

            <!-- HJELP -->
            <div class="page" id="page-hjelp">
                <div class="help-content">
                    <h2>Brukerveiledning</h2>

                    <section>
                        <h3>Hva er dette dashbordet?</h3>
                        <p>Mobilitetsdashbordet gir deg oversikt over hvordan folk reiser i Tromsø kommune. Her kan du se utviklingen i køer, forsinkelser og reisemønstre over tid. Dashbordet er nyttig for alle som er interessert i trafikk og mobilitet i Tromsø – enten du jobber med byplanlegging, er politiker, eller bare nysgjerrig på hvordan trafikken utvikler seg.</p>
                    </section>

                    <section>
                        <h3>Slik navigerer du</h3>
                        <p>Øverst på siden finner du en meny med følgende valg:</p>
                        <ul>
                            <li><strong>Hjem</strong> – Startsiden med oversikt over alle funksjoner</li>
                            <li><strong>Forsinkelser og køer</strong> – Se køsituasjonen på veiene</li>
                            <li><strong>Reisestatistikk Tromsø sentrum</strong> – Se hvor mange som reiser til og fra sentrum</li>
                            <li><strong>Reisestrømmer i Tromsø kommune</strong> – Utforsk reisemønstre mellom ulike områder</li>
                            <li><strong>Kart</strong> – Interaktivt kart over Tromsø</li>
                        </ul>
                        <p>Du kan også klikke direkte på boksene på startsiden for å komme til de ulike sidene.</p>
                    </section>

                    <section>
                        <h3>Forsinkelser og køer</h3>
                        <p>Her kan du se hvor mye kø det er på utvalgte strekninger i Tromsø, og hvor mye bussene blir forsinket.</p>
                        <p><strong>Slik bruker du siden:</strong></p>
                        <p>I menyen til venstre kan du velge:</p>
                        <ol>
                            <li><strong>Strekning</strong> – Velg en spesifikk veistrekning, eller «Alle strekninger» for å se gjennomsnittet</li>
                            <li><strong>Visning</strong> – Velg mellom å se køindeks (minutter forsinkelse per kilometer) eller bussforsinkelser</li>
                            <li><strong>Vis over</strong> – Velg om du vil se utviklingen over tid (dato) eller fordelt på klokkeslett</li>
                            <li><strong>Tid på døgnet</strong> – Velg morgenrush eller ettermiddagsrush</li>
                        </ol>
                        <p><em>Tips: Prøv å bytte mellom «Over dato» og «Over klokkeslett» for å se når på dagen køene er verst.</em></p>
                    </section>

                    <section>
                        <h3>Reisestatistikk Tromsø sentrum</h3>
                        <p>Her ser du hvor mange reiser som går til og fra Tromsø sentrum, fordelt på transportmiddel.</p>
                        <p><strong>Slik bruker du siden:</strong></p>
                        <p>Velg en strekning i menyen til venstre. Grafen viser antall reiser per kvartal, delt opp på bil, buss, sykkel og gange.</p>
                        <p><em>Tips: Hold musepekeren over grafen for å se nøyaktige tall for hvert kvartal.</em></p>
                    </section>

                    <section>
                        <h3>Reisestrømmer i Tromsø kommune</h3>
                        <p>Her kan du utforske hvordan folk reiser mellom ulike områder i kommunen.</p>
                        <p><strong>Slik bruker du siden:</strong></p>
                        <p>I menyen til venstre kan du filtrere på:</p>
                        <ol>
                            <li><strong>Område fra</strong> – Hvor reisene starter (hold Ctrl og klikk for å velge flere)</li>
                            <li><strong>Område til</strong> – Hvor reisene ender</li>
                            <li><strong>Tid på dagen</strong> – Morgen, formiddag, ettermiddag, kveld eller natt</li>
                            <li><strong>Ukedag/helg</strong> – Se forskjellen mellom hverdager og helger</li>
                        </ol>
                        <p>Grafen viser antall reiser per kvartal. De svake prikkene er faktiske tall, mens linjen viser trenden (glidende gjennomsnitt).</p>
                        <p><strong>Ekstra funksjoner:</strong></p>
                        <ul>
                            <li><strong>Vis reisestrømmer</strong> – Når du har valgt spesifikke områder, kan du klikke på denne knappen for å se et flytdiagram som viser hvor reisene går</li>
                            <li><strong>Eksporter CSV</strong> – Last ned dataene til Excel for videre analyse</li>
                        </ul>
                    </section>

                    <section>
                        <h3>Kart</h3>
                        <p>Her finner du et interaktivt kart over Tromsø sentrum og omegn.</p>
                        <p>Klikk på bildet eller knappen for å åpne kartet i en ny fane. I kartet kan du slå av og på ulike lag som viser:</p>
                        <ul>
                            <li>Trafikk til Tromsø sentrum (morgen)</li>
                            <li>Trafikk fra Tromsø sentrum (ettermiddag)</li>
                            <li>Gjennomfartstrafikk</li>
                            <li>Oversikt over køer</li>
                            <li>Soner og områdeinndeling</li>
                        </ul>
                    </section>

                    <section>
                        <h3>Ofte stilte spørsmål</h3>
                        <p><strong>Hvor kommer dataene fra?</strong><br>
                        Dataene er basert på mobildata og trafikkregistreringer, og oppdateres kvartalsvis.</p>

                        <p><strong>Hva betyr «kø (min/km)»?</strong><br>
                        Dette er en køindeks som viser hvor mange ekstra minutter du bruker per kilometer sammenlignet med fri flyt. Høyere tall betyr mer kø.</p>

                        <p><strong>Kan jeg laste ned dataene?</strong><br>
                        Ja, på siden «Reisestrømmer i Tromsø kommune» kan du klikke «Eksporter CSV» for å laste ned de filtrerte dataene.</p>

                        <p><strong>Fungerer dashbordet på mobil?</strong><br>
                        Ja, dashbordet tilpasser seg skjermstørrelsen, men det er enklest å bruke på en større skjerm.</p>
                    </section>

                    <section class="contact-section">
                        <h3>Kontakt</h3>
                        <p>Har du spørsmål eller tilbakemeldinger om dashbordet?</p>
                        <p><strong>Arve Halseth</strong><br>
                        E-post: <a href="mailto:aha@jaleas.no">aha@jaleas.no</a><br>
                        Telefon: 99 69 86 61</p>
                    </section>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Embedded data
        const koData = {json.dumps(ko_aggregated, ensure_ascii=False)};
        const reiserData = {json.dumps(reiser_dict, ensure_ascii=False)};
        const nokkelData = {json.dumps(nokkel_data, ensure_ascii=False)};

        // Første datoer med data
        const firstKoDate = '{first_ko_date}';
        const firstForsinkelserDate = '{first_forsinkelser_date}';

        // Initialiser startdato ved sidelast
        document.addEventListener('DOMContentLoaded', function() {{
            initStartdatoFilter();
        }});

        function initStartdatoFilter() {{
            const visning = document.querySelector('input[name="visning"]:checked').value;
            const startdatoInput = document.getElementById('startdato-ko');
            const hintEl = document.getElementById('startdato-hint');

            if (visning === 'ko') {{
                startdatoInput.min = firstKoDate;
                startdatoInput.value = firstKoDate;
                hintEl.textContent = 'Kø-data tilgjengelig fra ' + formatDateNorwegian(firstKoDate);
            }} else {{
                startdatoInput.min = firstForsinkelserDate;
                startdatoInput.value = firstForsinkelserDate;
                hintEl.textContent = 'Forsinkelser tilgjengelig fra ' + formatDateNorwegian(firstForsinkelserDate);
            }}
        }}

        function onVisningChange() {{
            initStartdatoFilter();
            updateKoChart();
        }}

        function formatDateNorwegian(isoDate) {{
            const parts = isoDate.split('-');
            return parts[2] + '.' + parts[1] + '.' + parts[0];
        }}

        // Navigation
        function showPage(page) {{
            document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
            document.querySelectorAll('.sidebar').forEach(s => s.style.display = 'none');
            document.querySelectorAll('.nav button').forEach(b => b.classList.remove('active'));

            document.getElementById('page-' + page).classList.add('active');
            event.target.classList.add('active');

            if (page === 'forsinkelser') {{
                document.getElementById('sidebar-forsinkelser').style.display = 'block';
                updateKoChart();
            }} else if (page === 'reisestatistikk') {{
                document.getElementById('sidebar-reisestatistikk').style.display = 'block';
                updateReiserChart();
            }} else if (page === 'nokkeltall') {{
                document.getElementById('sidebar-nokkeltall').style.display = 'block';
                updateNokkelChart();
            }}
        }}

        // Navigering fra hjem-kort
        function navigateTo(page) {{
            document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
            document.querySelectorAll('.sidebar').forEach(s => s.style.display = 'none');
            document.querySelectorAll('.nav button').forEach(b => b.classList.remove('active'));

            document.getElementById('page-' + page).classList.add('active');

            // Finn og marker riktig navigasjonsknapp
            const navButtons = document.querySelectorAll('.nav button');
            navButtons.forEach(btn => {{
                if (btn.getAttribute('onclick') === "showPage('" + page + "')") {{
                    btn.classList.add('active');
                }}
            }});

            if (page === 'forsinkelser') {{
                document.getElementById('sidebar-forsinkelser').style.display = 'block';
                updateKoChart();
            }} else if (page === 'reisestatistikk') {{
                document.getElementById('sidebar-reisestatistikk').style.display = 'block';
                updateReiserChart();
            }} else if (page === 'nokkeltall') {{
                document.getElementById('sidebar-nokkeltall').style.display = 'block';
                updateNokkelChart();
            }}
        }}

        // Kø/Forsinkelser chart
        function updateKoChart() {{
            const strekningSelect = document.getElementById('strekning-ko');
            let valgteStrekninger = Array.from(strekningSelect.selectedOptions).map(o => o.value);
            const alleStrekningerValgt = valgteStrekninger.includes('Alle strekninger') || valgteStrekninger.length === 0;

            const visning = document.querySelector('input[name="visning"]:checked').value;
            const xakse = document.querySelector('input[name="xakse"]:checked').value;
            const tid = document.querySelector('input[name="tid"]:checked').value;
            const startdato = document.getElementById('startdato-ko').value;

            const farger = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880'];
            const traces = [];

            // Bestem hvilke strekninger som skal vises
            const strekningerÅVise = alleStrekningerValgt ? ['Alle strekninger'] : valgteStrekninger;

            if (xakse === 'dato') {{
                // Samle alle unike datoer fra alle valgte strekninger
                const alleDatoerSet = new Set();
                const strekningData = {{}};

                strekningerÅVise.forEach(strekning => {{
                    const dataKey = strekning + '_' + tid;
                    if (!koData[dataKey]) return;

                    const datoerIso = koData[dataKey].datoer_iso;
                    const alleDatoer = koData[dataKey].datoer;
                    const alleY = visning === 'ko' ? koData[dataKey].ko : koData[dataKey].forsinkelser;

                    // Lag mapping fra ISO-dato til verdi
                    const datoMap = {{}};
                    const datoStrMap = {{}};
                    for (let i = 0; i < datoerIso.length; i++) {{
                        if (datoerIso[i] >= startdato) {{
                            alleDatoerSet.add(datoerIso[i]);
                            datoMap[datoerIso[i]] = alleY[i];
                            datoStrMap[datoerIso[i]] = alleDatoer[i];
                        }}
                    }}
                    strekningData[strekning] = {{ datoMap, datoStrMap }};
                }});

                // Sorter alle datoer
                const sorterteDatoerIso = Array.from(alleDatoerSet).sort();

                // Lag mapping fra ISO til visningsdato (bruk første tilgjengelige)
                const isoTilVisning = {{}};
                sorterteDatoerIso.forEach(iso => {{
                    for (const strekning of strekningerÅVise) {{
                        if (strekningData[strekning] && strekningData[strekning].datoStrMap[iso]) {{
                            isoTilVisning[iso] = strekningData[strekning].datoStrMap[iso];
                            break;
                        }}
                    }}
                }});

                const xDataFelles = sorterteDatoerIso.map(iso => isoTilVisning[iso]);

                strekningerÅVise.forEach((strekning, idx) => {{
                    if (!strekningData[strekning]) return;

                    const datoMap = strekningData[strekning].datoMap;
                    const yData = sorterteDatoerIso.map(iso => datoMap[iso] !== undefined ? datoMap[iso] : null);
                    const farge = farger[idx % farger.length];

                    if (visning === 'ko') {{
                        // For kø: vis prikker + trendlinje
                        const trend = beregnGlidendeGjennomsnitt(yData, 7);

                        // Rådata som punkter (uten legend)
                        traces.push({{
                            x: xDataFelles,
                            y: yData,
                            type: 'scatter',
                            mode: 'markers',
                            name: strekning,
                            marker: {{ color: farge, size: 5, opacity: 0.6 }},
                            showlegend: false
                        }});

                        // Trendlinje (med legend)
                        traces.push({{
                            x: xDataFelles,
                            y: trend,
                            type: 'scatter',
                            mode: 'lines',
                            name: strekning,
                            line: {{ color: farge, width: 2, shape: 'spline', smoothing: 1.0 }},
                            connectgaps: true
                        }});
                    }} else {{
                        // For forsinkelser: kun linje
                        traces.push({{
                            x: xDataFelles,
                            y: yData,
                            type: 'scatter',
                            mode: 'lines',
                            name: strekning,
                            line: {{ color: farge }},
                            connectgaps: true
                        }});
                    }}
                }});

                const yLabel = visning === 'ko' ? 'Kø (min/km)' : 'Forsinkelser (min)';
                const titleStrekninger = alleStrekningerValgt ? 'alle strekninger' : strekningerÅVise.join(', ').toLowerCase();
                const title = (visning === 'ko' ? 'Kø' : 'Forsinkelser buss') + ' - ' + titleStrekninger + ' (' + tid.toLowerCase() + ')';

                const layout = {{
                    title: title,
                    xaxis: {{ 
                        title: 'Dato', 
                        tickangle: -45,
                        type: 'category'
                    }},
                    yaxis: {{ title: yLabel, rangemode: 'tozero' }},
                    hovermode: 'x unified',
                    showlegend: !alleStrekningerValgt && strekningerÅVise.length > 1
                }};

                Plotly.newPlot('ko-chart', traces, layout, {{responsive: true}});

            }} else {{
                // Klokkeslett-visning med datofilter
                // Samle alle unike klokkeslett
                const alleKlokkeslettSet = new Set();
                const strekningKlData = {{}};

                strekningerÅVise.forEach(strekning => {{
                    const rawKey = strekning + '_' + tid + '_klokkeslett_raw';
                    if (!koData[rawKey] || !koData[rawKey].records) return;

                    // Filtrer records basert på startdato
                    const filteredRecords = koData[rawKey].records.filter(r => r.dato_iso >= startdato);

                    // Aggreger per klokkeslett
                    const klokkeslettData = {{}};
                    filteredRecords.forEach(r => {{
                        const kl = r.klokkeslett;
                        alleKlokkeslettSet.add(kl);
                        const val = visning === 'ko' ? r.ko : r.forsinkelser;
                        if (val !== null) {{
                            if (!klokkeslettData[kl]) {{
                                klokkeslettData[kl] = [];
                            }}
                            klokkeslettData[kl].push(val);
                        }}
                    }});

                    strekningKlData[strekning] = klokkeslettData;
                }});

                // Sorter klokkeslett
                const sorterteKlokkeslett = Array.from(alleKlokkeslettSet).sort();

                strekningerÅVise.forEach((strekning, idx) => {{
                    if (!strekningKlData[strekning]) return;

                    const klokkeslettData = strekningKlData[strekning];
                    const yData = sorterteKlokkeslett.map(kl => {{
                        const vals = klokkeslettData[kl];
                        if (!vals || vals.length === 0) return null;
                        const sum = vals.reduce((a, b) => a + b, 0);
                        return Math.round(sum / vals.length * 1000) / 1000;
                    }});

                    const farge = farger[idx % farger.length];

                    traces.push({{
                        x: sorterteKlokkeslett,
                        y: yData,
                        type: 'bar',
                        name: strekning,
                        marker: {{ color: farge }}
                    }});
                }});

                const yLabel = visning === 'ko' ? 'Kø (min/km)' : 'Forsinkelser (min)';
                const titleStrekninger = alleStrekningerValgt ? 'alle strekninger' : strekningerÅVise.join(', ').toLowerCase();
                const title = (visning === 'ko' ? 'Kø' : 'Forsinkelser buss') + ' - ' + titleStrekninger + ' (' + tid.toLowerCase() + ')';

                const layout = {{
                    title: title,
                    xaxis: {{ 
                        title: 'Klokkeslett', 
                        tickangle: -45,
                        type: 'category'
                    }},
                    yaxis: {{ title: yLabel, rangemode: 'tozero' }},
                    hovermode: 'x unified',
                    showlegend: !alleStrekningerValgt && strekningerÅVise.length > 1,
                    barmode: 'group'
                }};

                Plotly.newPlot('ko-chart', traces, layout, {{responsive: true}});
            }}
        }}

        // Funksjon for å beregne sentrert glidende gjennomsnitt
        function beregnGlidendeGjennomsnitt(values, windowSize) {{
            const result = [];
            const halfWindow = Math.floor(windowSize / 2);

            for (let i = 0; i < values.length; i++) {{
                // Beregn start og slutt for vinduet (sentrert)
                let start = Math.max(0, i - halfWindow);
                let end = Math.min(values.length - 1, i + halfWindow);

                // Samle verdier i vinduet (ignorer null/undefined)
                let sum = 0;
                let count = 0;
                for (let j = start; j <= end; j++) {{
                    if (values[j] != null && !isNaN(values[j])) {{
                        sum += values[j];
                        count++;
                    }}
                }}

                // Beregn gjennomsnitt hvis vi har minst 1 verdi
                if (count > 0) {{
                    result.push(Math.round(sum / count * 100) / 100);
                }} else {{
                    result.push(null);
                }}
            }}

            return result;
        }}

        // Reisestatistikk chart
        function updateReiserChart() {{
            const strekning = document.getElementById('strekning-reiser').value;
            const data = reiserData[strekning];
            if (!data) return;

            const transportmiddelSelect = document.getElementById('transportmiddel');
            let valgteModi = Array.from(transportmiddelSelect.selectedOptions).map(o => o.value);
            const alleValgt = valgteModi.includes('Alle') || valgteModi.length === 0;

            const colors = {{
                'bil': '#636EFA',
                'buss': '#EF553B',
                'sykkel': '#00CC96',
                'gange': '#AB63FA'
            }};

            const labels = {{
                'bil': 'Bil',
                'buss': 'Buss',
                'sykkel': 'Sykkel',
                'gange': 'Gange'
            }};

            const alleModi = ['bil', 'buss', 'sykkel', 'gange'];
            const traces = [];

            if (alleValgt) {{
                // Vis kun trendlinjer for alle transportmidler
                alleModi.forEach(mode => {{
                    const trend = beregnGlidendeGjennomsnitt(data[mode], 5);
                    traces.push({{
                        name: labels[mode],
                        x: data.kvartaler,
                        y: trend,
                        type: 'scatter',
                        mode: 'lines',
                        line: {{ color: colors[mode], width: 2, shape: 'spline', smoothing: 1.0 }},
                        connectgaps: true
                    }});
                }});
            }} else {{
                // Vis rådata som prikker + trendlinje for valgte modi
                valgteModi.forEach(mode => {{
                    if (mode === 'Alle') return;

                    const trend = beregnGlidendeGjennomsnitt(data[mode], 5);

                    // Rådata som punkter (uten legend)
                    traces.push({{
                        name: labels[mode],
                        x: data.kvartaler,
                        y: data[mode],
                        type: 'scatter',
                        mode: 'markers',
                        marker: {{ color: colors[mode], size: 5, opacity: 0.6 }},
                        showlegend: false
                    }});

                    // Trendlinje (med legend)
                    traces.push({{
                        name: labels[mode],
                        x: data.kvartaler,
                        y: trend,
                        type: 'scatter',
                        mode: 'lines',
                        line: {{ color: colors[mode], width: 2, shape: 'spline', smoothing: 1.0 }},
                        connectgaps: true
                    }});
                }});
            }}

            const titleSuffix = alleValgt ? ' - trend' : ' - ' + valgteModi.filter(m => m !== 'Alle').map(m => labels[m]).join(', ');

            const layout = {{
                title: 'Reisestatistikk - ' + strekning + titleSuffix + ' (1000 reiser per kvartal)',
                xaxis: {{ title: 'Kvartal', tickangle: -45, type: 'category' }},
                yaxis: {{ title: 'Antall reiser (1000 per kvartal)', rangemode: 'tozero' }},
                hovermode: 'x unified',
                legend: {{ title: {{ text: 'Transportmiddel' }} }}
            }};

            Plotly.newPlot('reiser-chart', traces, layout, {{responsive: true}});
        }}

        // Global variabel for CSV-eksport
        let csvExportData = [];

        // Nøkkeltall reiser chart
        function updateNokkelChart() {{
            const omradeFraSelect = document.getElementById('omrade-fra');
            const omradeTilSelect = document.getElementById('omrade-til');
            const tidNokkel = document.querySelector('input[name="tid-nokkel"]:checked').value;
            const ukedagNokkel = document.querySelector('input[name="ukedag-nokkel"]:checked').value;
            const visningNokkel = document.querySelector("input[name='visning-nokkel']:checked").value;

            // Hent valgte områder (rå valg fra dropdown)
            let fraValg = Array.from(omradeFraSelect.selectedOptions).map(o => o.value);
            let tilValg = Array.from(omradeTilSelect.selectedOptions).map(o => o.value);

            const fraAlleValgt = fraValg.includes('Alle') || fraValg.length === 0;
            const tilAlleValgt = tilValg.includes('Alle') || tilValg.length === 0;

            // Bestem hvilke områder som skal brukes for filtrering
            let omraderFra = fraAlleValgt ? nokkelData.omrader_fra : fraValg;
            let omraderTil = tilAlleValgt ? nokkelData.omrader_til : tilValg;

            // Bestem om vi skal splitte til flere linjer
            // Prioriter fra-områder hvis begge har flervalg
            let splitPå = null;
            let splitOmrader = [];

            if (!fraAlleValgt && fraValg.length > 1) {{
                splitPå = 'fra';
                splitOmrader = fraValg;
            }} else if (!tilAlleValgt && tilValg.length > 1) {{
                splitPå = 'til';
                splitOmrader = tilValg;
            }}

            // Filtrer data
            let filtered = nokkelData.records.filter(r => {{
                const fraMatch = omraderFra.includes(r.delomrade_fra);
                const tilMatch = omraderTil.includes(r.delomrade_til);
                const tidMatch = tidNokkel === 'Alle' || r.time_of_day === tidNokkel;
                const ukedagMatch = ukedagNokkel === 'Alle' || r.weekday_indicator === ukedagNokkel;
                return fraMatch && tilMatch && tidMatch && ukedagMatch;
            }});

            const traces = [];
            csvExportData = [];
            const farger = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52'];

            if (splitPå === 'fra') {{
                // Flere linjer - én per fra-område
                splitOmrader.forEach((omrade, idx) => {{
                    const omradeFiltered = filtered.filter(r => r.delomrade_fra === omrade);

                    // Aggreger per kvartal - summer både reiser og CO2
                    const kvartalData = {{}};
                    omradeFiltered.forEach(r => {{
                        if (!kvartalData[r.kvartal]) {{
                            kvartalData[r.kvartal] = {{ reiser: 0, co2: 0 }};
                        }}
                        kvartalData[r.kvartal].reiser += r.reiser || 0;
                        kvartalData[r.kvartal].co2 += r.co2_tonn || 0;
                    }});

                    const sortedKvartaler = nokkelData.kvartaler.filter(k => kvartalData[k] !== undefined);

                    // Velg y-verdier basert på visning
                    let yValues;
                    if (visningNokkel === 'co2_per_reise') {{
                        // CO2 per reise (tonn / 1000 reiser = kg per reise)
                        yValues = sortedKvartaler.map(k => {{
                            const d = kvartalData[k];
                            if (d.reiser > 0) {{
                                return Math.round(d.co2 / d.reiser * 100) / 100;
                            }}
                            return null;
                        }});
                    }} else if (visningNokkel === 'co2_sum') {{
                        yValues = sortedKvartaler.map(k => Math.round(kvartalData[k].co2 * 100) / 100);
                    }} else {{
                        yValues = sortedKvartaler.map(k => Math.round(kvartalData[k].reiser * 100) / 100);
                    }}

                    // Beregn trend ETTER aggregering
                    const trendValues = beregnGlidendeGjennomsnitt(yValues, 5);

                    const farge = farger[idx % farger.length];

                    // Rådata som punkter (uten legend)
                    traces.push({{
                        x: sortedKvartaler,
                        y: yValues,
                        type: 'scatter',
                        mode: 'markers',
                        name: omrade,
                        marker: {{ color: farge, size: 5, opacity: 0.6 }},
                        showlegend: false
                    }});

                    // Trend som linje
                    traces.push({{
                        x: sortedKvartaler,
                        y: trendValues,
                        type: 'scatter',
                        mode: 'lines',
                        name: omrade,
                        line: {{ color: farge, width: 2, shape: 'spline', smoothing: 1.0 }},
                        connectgaps: true
                    }});

                    // Lagre for CSV - inkluder både reiser og CO2
                    const tilOmraderTekst = tilAlleValgt ? 'Alle' : tilValg.join(', ');
                    sortedKvartaler.forEach((k, i) => {{
                        const d = kvartalData[k];
                        const reiserTrend = beregnGlidendeGjennomsnitt(sortedKvartaler.map(kv => Math.round(kvartalData[kv].reiser * 100) / 100), 5);
                        const co2PerReise = d.reiser > 0 ? Math.round(d.co2 / d.reiser * 100) / 100 : null;
                        const co2PerReiseTrend = beregnGlidendeGjennomsnitt(sortedKvartaler.map(kv => {{
                            const kd = kvartalData[kv];
                            return kd.reiser > 0 ? Math.round(kd.co2 / kd.reiser * 100) / 100 : null;
                        }}), 5);
                        csvExportData.push({{
                            område_fra: omrade,
                            område_til: tilOmraderTekst,
                            kvartal: k,
                            reiser: Math.round(d.reiser * 100) / 100,
                            reiser_trend: reiserTrend[i],
                            co2_tonn: Math.round(d.co2 * 100) / 100,
                            co2_per_reise: co2PerReise,
                            co2_per_reise_trend: co2PerReiseTrend[i]
                        }});
                    }});
                }});
            }} else if (splitPå === 'til') {{
                // Flere linjer basert på til-områder
                splitOmrader.forEach((omrade, idx) => {{
                    const omradeFiltered = filtered.filter(r => r.delomrade_til === omrade);

                    // Aggreger per kvartal
                    const kvartalData = {{}};
                    omradeFiltered.forEach(r => {{
                        if (!kvartalData[r.kvartal]) {{
                            kvartalData[r.kvartal] = {{ reiser: 0, co2: 0 }};
                        }}
                        kvartalData[r.kvartal].reiser += r.reiser || 0;
                        kvartalData[r.kvartal].co2 += r.co2_tonn || 0;
                    }});

                    const sortedKvartaler = nokkelData.kvartaler.filter(k => kvartalData[k] !== undefined);

                    // Velg y-verdier basert på visning
                    let yValues;
                    if (visningNokkel === 'co2_per_reise') {{
                        yValues = sortedKvartaler.map(k => {{
                            const d = kvartalData[k];
                            if (d.reiser > 0) {{
                                return Math.round(d.co2 / d.reiser * 100) / 100;
                            }}
                            return null;
                        }});
                    }} else if (visningNokkel === 'co2_sum') {{
                        yValues = sortedKvartaler.map(k => Math.round(kvartalData[k].co2 * 100) / 100);
                    }} else {{
                        yValues = sortedKvartaler.map(k => Math.round(kvartalData[k].reiser * 100) / 100);
                    }}

                    // Beregn trend ETTER aggregering
                    const trendValues = beregnGlidendeGjennomsnitt(yValues, 5);

                    const farge = farger[idx % farger.length];

                    // Rådata som punkter (uten legend)
                    traces.push({{
                        x: sortedKvartaler,
                        y: yValues,
                        type: 'scatter',
                        mode: 'markers',
                        name: omrade,
                        marker: {{ color: farge, size: 5, opacity: 0.6 }},
                        showlegend: false
                    }});

                    // Trend som linje
                    traces.push({{
                        x: sortedKvartaler,
                        y: trendValues,
                        type: 'scatter',
                        mode: 'lines',
                        name: omrade,
                        line: {{ color: farge, width: 2, shape: 'spline', smoothing: 1.0 }},
                        connectgaps: true
                    }});

                    // Lagre for CSV
                    const fraOmraderTekst = fraAlleValgt ? 'Alle' : fraValg.join(', ');
                    sortedKvartaler.forEach((k, i) => {{
                        const d = kvartalData[k];
                        const reiserTrend = beregnGlidendeGjennomsnitt(sortedKvartaler.map(kv => Math.round(kvartalData[kv].reiser * 100) / 100), 5);
                        const co2PerReise = d.reiser > 0 ? Math.round(d.co2 / d.reiser * 100) / 100 : null;
                        const co2PerReiseTrend = beregnGlidendeGjennomsnitt(sortedKvartaler.map(kv => {{
                            const kd = kvartalData[kv];
                            return kd.reiser > 0 ? Math.round(kd.co2 / kd.reiser * 100) / 100 : null;
                        }}), 5);
                        csvExportData.push({{
                            område_fra: fraOmraderTekst,
                            område_til: omrade,
                            kvartal: k,
                            reiser: Math.round(d.reiser * 100) / 100,
                            reiser_trend: reiserTrend[i],
                            co2_tonn: Math.round(d.co2 * 100) / 100,
                            co2_per_reise: co2PerReise,
                            co2_per_reise_trend: co2PerReiseTrend[i]
                        }});
                    }});
                }});
            }} else {{
                // Én samlet linje
                const kvartalData = {{}};
                filtered.forEach(r => {{
                    if (!kvartalData[r.kvartal]) {{
                        kvartalData[r.kvartal] = {{ reiser: 0, co2: 0 }};
                    }}
                    kvartalData[r.kvartal].reiser += r.reiser || 0;
                    kvartalData[r.kvartal].co2 += r.co2_tonn || 0;
                }});

                const sortedKvartaler = nokkelData.kvartaler.filter(k => kvartalData[k] !== undefined);

                // Velg y-verdier basert på visning
                let yValues;
                if (visningNokkel === 'co2_per_reise') {{
                    yValues = sortedKvartaler.map(k => {{
                        const d = kvartalData[k];
                        if (d.reiser > 0) {{
                            return Math.round(d.co2 / d.reiser * 100) / 100;
                        }}
                        return null;
                    }});
                }} else if (visningNokkel === 'co2_sum') {{
                    yValues = sortedKvartaler.map(k => Math.round(kvartalData[k].co2 * 100) / 100);
                }} else {{
                    yValues = sortedKvartaler.map(k => Math.round(kvartalData[k].reiser * 100) / 100);
                }}

                // Beregn trend ETTER aggregering
                const trendValues = beregnGlidendeGjennomsnitt(yValues, 5);

                traces.push({{
                    x: sortedKvartaler,
                    y: yValues,
                    type: 'scatter',
                    mode: 'markers',
                    name: 'Rådata',
                    marker: {{ color: '#636EFA', size: 5, opacity: 0.6 }},
                    showlegend: false
                }});

                traces.push({{
                    x: sortedKvartaler,
                    y: trendValues,
                    type: 'scatter',
                    mode: 'lines',
                    name: 'Trend',
                    line: {{ color: '#636EFA', width: 2, shape: 'spline', smoothing: 1.0 }},
                    connectgaps: true
                }});

                // Lagre for CSV
                const fraOmraderTekst = fraAlleValgt ? 'Alle' : fraValg.join(', ');
                const tilOmraderTekst = tilAlleValgt ? 'Alle' : tilValg.join(', ');
                const reiserValues = sortedKvartaler.map(k => Math.round(kvartalData[k].reiser * 100) / 100);
                const reiserTrend = beregnGlidendeGjennomsnitt(reiserValues, 5);
                const co2PerReiseValues = sortedKvartaler.map(k => {{
                    const d = kvartalData[k];
                    return d.reiser > 0 ? Math.round(d.co2 / d.reiser * 100) / 100 : null;
                }});
                const co2PerReiseTrend = beregnGlidendeGjennomsnitt(co2PerReiseValues, 5);

                sortedKvartaler.forEach((k, i) => {{
                    const d = kvartalData[k];
                    csvExportData.push({{
                        område_fra: fraOmraderTekst,
                        område_til: tilOmraderTekst,
                        kvartal: k,
                        reiser: reiserValues[i],
                        reiser_trend: reiserTrend[i],
                        co2_tonn: Math.round(d.co2 * 100) / 100,
                        co2_per_reise: co2PerReiseValues[i],
                        co2_per_reise_trend: co2PerReiseTrend[i]
                    }});
                }});
            }}

            // Dynamisk tittel og y-akse basert på visning
            let titleText, yAxisLabel;
            if (visningNokkel === 'co2_per_reise') {{
                titleText = 'CO2-utslipp per reise i Tromsø kommune';
                yAxisLabel = 'CO2 (kg per reise)';
            }} else if (visningNokkel === 'co2_sum') {{
                titleText = 'CO2-utslipp i Tromsø kommune - sum per kvartal';
                yAxisLabel = 'CO2 (tonn per kvartal)';
            }} else {{
                titleText = 'Reisestrømmer i Tromsø kommune - sum reiser per kvartal';
                yAxisLabel = 'Antall reiser (1000 per kvartal)';
            }}

            const layout = {{
                title: titleText,
                xaxis: {{ title: 'Kvartal', tickangle: -45, type: 'category' }},
                yaxis: {{ title: yAxisLabel, rangemode: 'tozero' }},
                hovermode: 'x unified',
                legend: {{ x: 0, y: 1.15, orientation: 'h' }}
            }};

            Plotly.newPlot('nokkel-chart', traces, layout, {{responsive: true}});

            // Vis/skjul sankey-knapp basert på filter (kun for reiser, ikke CO2)
            const sankeyBtn = document.getElementById('sankey-btn');
            if ((fraAlleValgt && tilAlleValgt) || visningNokkel === 'co2_sum' || visningNokkel === 'co2_per_reise') {{
                sankeyBtn.style.display = 'none';
            }} else {{
                sankeyBtn.style.display = 'inline-block';
            }}
        }}

        // CSV eksport funksjon
        function exportCSV() {{
            if (csvExportData.length === 0) {{
                alert('Ingen data å eksportere');
                return;
            }}

            // Header med alle kolonner
            const headers = ['Område fra', 'Område til', 'Kvartal', 'Reiser (1000)', 'Reiser trend', 'CO2 (tonn)', 'CO2 per reise (kg)', 'CO2 per reise trend'];
            const csvContent = [
                headers.join(';'),
                ...csvExportData.map(row => [
                    row.område_fra,
                    row.område_til,
                    row.kvartal,
                    row.reiser != null ? String(row.reiser).replace('.', ',') : '',
                    row.reiser_trend != null ? String(row.reiser_trend).replace('.', ',') : '',
                    row.co2_tonn != null ? String(row.co2_tonn).replace('.', ',') : '',
                    row.co2_per_reise != null ? String(row.co2_per_reise).replace('.', ',') : '',
                    row.co2_per_reise_trend != null ? String(row.co2_per_reise_trend).replace('.', ',') : ''
                ].join(';'))
            ].join('\\n');

            // Last ned fil med UTF-8 BOM for Excel
            const BOM = '\\uFEFF';
            const blob = new Blob([BOM + csvContent], {{ type: 'text/csv;charset=utf-8;' }});
            const link = document.createElement('a');
            link.href = URL.createObjectURL(blob);
            link.download = 'reisestrommer_Tromsø.csv';
            link.click();
        }}

        // Sankey modal funksjoner
        function openSankeyModal() {{
            const fraValg = Array.from(document.getElementById('omrade-fra').selectedOptions).map(o => o.value);
            const tilValg = Array.from(document.getElementById('omrade-til').selectedOptions).map(o => o.value);
            const fraAlleValgt = fraValg.includes('Alle') || fraValg.length === 0;
            const tilAlleValgt = tilValg.includes('Alle') || tilValg.length === 0;

            const fraLabel = document.getElementById('sankey-fra-label');
            const tilLabel = document.getElementById('sankey-til-label');
            const fraRadio = fraLabel.querySelector('input');
            const tilRadio = tilLabel.querySelector('input');

            // Vis/skjul radioknapper basert på filtervalg
            if (fraAlleValgt) {{
                fraLabel.classList.add('disabled');
                tilLabel.classList.remove('disabled');
                tilRadio.checked = true;
            }} else if (tilAlleValgt) {{
                tilLabel.classList.add('disabled');
                fraLabel.classList.remove('disabled');
                fraRadio.checked = true;
            }} else {{
                fraLabel.classList.remove('disabled');
                tilLabel.classList.remove('disabled');
            }}

            document.getElementById('sankey-modal').style.display = 'block';
            updateSankeyChart();
        }}

        function closeSankeyModal() {{
            document.getElementById('sankey-modal').style.display = 'none';
        }}

        // Lukk modal ved klikk utenfor
        window.onclick = function(event) {{
            const modal = document.getElementById('sankey-modal');
            if (event.target === modal) {{
                modal.style.display = 'none';
            }}
        }}

        function updateSankeyChart() {{
            const omradeFraSelect = document.getElementById('omrade-fra');
            const omradeTilSelect = document.getElementById('omrade-til');
            const retning = document.querySelector('input[name="sankey-retning"]:checked').value;

            // Hent valgte områder
            let omraderFra = Array.from(omradeFraSelect.selectedOptions).map(o => o.value);
            let omraderTil = Array.from(omradeTilSelect.selectedOptions).map(o => o.value);

            // Sjekk om "Alle" er valgt
            const fraAlleValgt = omraderFra.includes('Alle') || omraderFra.length === 0;
            const tilAlleValgt = omraderTil.includes('Alle') || omraderTil.length === 0;

            // Finn siste 4 kvartaler
            const sisteKvartaler = nokkelData.kvartaler.slice(-4);

            // Filtrer data for siste 4 kvartaler
            let filtered = nokkelData.records.filter(r => sisteKvartaler.includes(r.kvartal));

            // Aggreger reiser per fra-til kombinasjon
            const strommer = {{}};
            let title = '';

            if (retning === 'fra') {{
                // Fra valgte områder til andre
                filtered.filter(r => omraderFra.includes(r.delomrade_fra)).forEach(r => {{
                    const key = r.delomrade_fra + '|' + r.delomrade_til;
                    if (!strommer[key]) {{
                        strommer[key] = {{ fra: r.delomrade_fra, til: r.delomrade_til, reiser: 0 }};
                    }}
                    strommer[key].reiser += r.reiser || 0;
                }});
                title = 'Reiser FRA valgte områder (topp 10 destinasjoner)';
            }} else {{
                // Fra andre til valgte områder
                filtered.filter(r => omraderTil.includes(r.delomrade_til)).forEach(r => {{
                    const key = r.delomrade_fra + '|' + r.delomrade_til;
                    if (!strommer[key]) {{
                        strommer[key] = {{ fra: r.delomrade_fra, til: r.delomrade_til, reiser: 0 }};
                    }}
                    strommer[key].reiser += r.reiser || 0;
                }});
                title = 'Reiser TIL valgte områder (topp 10 opprinnelser)';
            }}

            // Sorter og ta topp 10
            const topp10 = Object.values(strommer)
                .sort((a, b) => b.reiser - a.reiser)
                .slice(0, 10);

            if (topp10.length === 0) {{
                Plotly.newPlot('sankey-chart', [], {{
                    title: 'Ingen data for valgte filtre',
                    annotations: [{{
                        text: 'Velg områder i sidemenyen',
                        showarrow: false,
                        font: {{ size: 16 }}
                    }}]
                }});
                return;
            }}

            // Bygg Sankey-data
            const fraLabels = [...new Set(topp10.map(d => d.fra))];
            const tilLabels = [...new Set(topp10.map(d => d.til))];
            const alleLabels = [...fraLabels, ...tilLabels];

            // Farger - grønn for fra, blå for til
            const colors = [
                ...fraLabels.map(() => '#00CC96'),
                ...tilLabels.map(() => '#636EFA')
            ];

            // Link-data
            const sources = topp10.map(d => fraLabels.indexOf(d.fra));
            const targets = topp10.map(d => fraLabels.length + tilLabels.indexOf(d.til));
            const values = topp10.map(d => Math.round(d.reiser));

            // Link-farger med gradient-effekt
            const linkColors = topp10.map((d, i) => {{
                const hue = (i * 50) % 360;
                return `hsla(${{hue}}, 70%, 60%, 0.5)`;
            }});

            const trace = {{
                type: 'sankey',
                orientation: 'h',
                node: {{
                    pad: 20,
                    thickness: 30,
                    label: alleLabels,
                    color: colors
                }},
                link: {{
                    source: sources,
                    target: targets,
                    value: values,
                    color: linkColors
                }}
            }};

            const layout = {{
                title: title,
                font: {{ size: 12 }},
                annotations: [
                    {{ x: 0.0, y: 1.05, text: '<b>Fra</b>', showarrow: false, xref: 'paper', yref: 'paper', font: {{ color: '#00CC96' }} }},
                    {{ x: 1.0, y: 1.05, text: '<b>Til</b>', showarrow: false, xref: 'paper', yref: 'paper', font: {{ color: '#636EFA' }} }}
                ]
            }};

            Plotly.newPlot('sankey-chart', [trace], layout, {{responsive: true}});
        }}
    </script>
</body>
</html>
'''
    return html


def main():
    print("Laster kødata...")
    ko_data = load_and_process_ko_data("data/inndata_Asker_ko.csv")
    print(f"  - {len(ko_data)} rader")
    print(f"  - Datoer: {ko_data['dato'].min()} til {ko_data['dato'].max()}")
    print(f"  - Strekninger: {ko_data['stop_name'].nunique()}")

    print("\nLaster reisedata...")
    reiser_data = load_and_process_reiser_data("data/inndata_Asker_reiser.csv")
    print(f"  - {len(reiser_data)} rader")

    print("\nLaster nøkkeltalldata...")
    nokkel_df = load_and_process_nokkel_data("data/inndata_Asker_nokkel.csv")
    nokkel_data = prepare_nokkel_data(nokkel_df)
    print(f"  - {len(nokkel_df)} rader")
    print(f"  - Områder fra: {len(nokkel_data['omrader_fra'])}")
    print(f"  - Områder til: {len(nokkel_data['omrader_til'])}")
    print(f"  - Tidsperioder: {len(nokkel_data['tider'])}")

    print("\nAggregerer kødata...")
    ko_aggregated = aggregate_ko_data(ko_data)
    print(f"  - {len(ko_aggregated)} datasett generert")

    print("\nBeregner første datoer...")
    first_ko_date, first_forsinkelser_date = calculate_first_dates(ko_aggregated)
    print(f"  - Første kø-dato: {first_ko_date}")
    print(f"  - Første forsinkelser-dato: {first_forsinkelser_date}")

    print("\nGenererer HTML...")
    html = generate_html(ko_data, reiser_data, ko_aggregated, nokkel_data, first_ko_date, first_forsinkelser_date)

    import os
    os.makedirs("docs", exist_ok=True)

    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nFerdig! Generert: docs/index.html")
    print(f"Filstørrelse: {len(html) / 1024:.1f} KB")
    print("\nFor å publisere på GitHub Pages:")
    print("1. git add docs/")
    print("2. git commit -m 'Oppdatert dashboard'")
    print("3. git push")
    print("4. I GitHub: Settings → Pages → Source: 'Deploy from branch' → Branch: main, Folder: /docs")


if __name__ == "__main__":
    main()