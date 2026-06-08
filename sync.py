#!/usr/bin/env python3
"""
HA SQLite → InfluxDB 2 Backfill
Schema: identisch zur nativen HA InfluxDB-Integration (api_version: 2)

Measurement:  unit_of_measurement  (falls vorhanden), sonst entity_id
Tags:         domain, entity_id (short, ohne domain-prefix)
              + alle tags aus --extra-tags (key=value)
Fields:       value (float) ODER state (str), plus alle Attribute
Timestamp:    last_updated_ts in ns UTC

Idempotent: InfluxDB 2 überschreibt identische (measurement, tags, timestamp)-Kombis.
Inkrementell: STATE_FILE speichert den letzten exportierten Timestamp.
"""

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# ── Konfiguration ────────────────────────────────────────────────────────────

INFLUX_URL    = os.getenv("INFLUX_URL",    "https://influxdb.hl443.de")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "d15e4819fb951381")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "homeassistant")
HA_DB         = os.getenv("HA_DB",         "./home-assistant_v2.db")
STATE_FILE    = os.getenv("STATE_FILE",    "./last_export_ts.txt")

# Entspricht measurement_attr: unit_of_measurement (HA-Default)
# Auf "entity_id" setzen wenn du measurement_attr: entity_id verwendest
MEASUREMENT_ATTR = os.getenv("MEASUREMENT_ATTR", "unit_of_measurement")

BATCH_SIZE = 5000

# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

# Attribute die HA intern ignoriert / nicht schreibt
IGNORE_ATTRIBUTES = {
    "unit_of_measurement", "friendly_name", "icon", "entity_picture",
    "supported_features", "hidden", "assumed_state", "device_class",
    "state_class",
}

def escape_tag(v: str) -> str:
    """Line-Protocol Tag-Escaping: Komma, Leerzeichen, Gleichheitszeichen."""
    return str(v).replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")

def escape_measurement(v: str) -> str:
    return str(v).replace(",", r"\,").replace(" ", r"\ ")

def escape_field_key(v: str) -> str:
    return str(v).replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")

def field_value(v) -> str | None:
    """Konvertiert einen Python-Wert in einen InfluxDB Line-Protocol Field-Value-String."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return f"{v}i"
    if isinstance(v, float):
        return str(v)
    if isinstance(v, str):
        escaped = v.replace('"', r'\"').replace("\\", "\\\\")
        return f'"{escaped}"'
    return None

def build_line(measurement: str, tags: dict, fields: dict, ts_ns: int) -> str | None:
    if not fields:
        return None
    tag_str = ",".join(f"{escape_tag(k)}={escape_tag(v)}" for k, v in sorted(tags.items()))
    field_parts = []
    for k, v in fields.items():
        fv = field_value(v)
        if fv is not None:
            field_parts.append(f"{escape_field_key(k)}={fv}")
    if not field_parts:
        return None
    field_str = ",".join(field_parts)
    m = escape_measurement(measurement)
    if tag_str:
        return f"{m},{tag_str} {field_str} {ts_ns}"
    return f"{m} {field_str} {ts_ns}"

# ── DB-Abfrage ───────────────────────────────────────────────────────────────

QUERY = """
    SELECT
        s.last_updated_ts,
        sm.entity_id,
        s.state,
        sa.shared_attrs
    FROM states s
    JOIN states_meta sm ON s.metadata_id = sm.metadata_id
    LEFT JOIN state_attributes sa ON s.attributes_id = sa.attributes_id
    WHERE s.state NOT IN ('unavailable', 'unknown')
      AND s.last_updated_ts IS NOT NULL
      {where_extra}
    ORDER BY s.last_updated_ts
"""

def fetch_rows(db_path: str, from_ts: float | None, to_ts: float | None):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    clauses, params = [], []
    if from_ts is not None:
        clauses.append("AND s.last_updated_ts > ?")
        params.append(from_ts)
    if to_ts is not None:
        clauses.append("AND s.last_updated_ts <= ?")
        params.append(to_ts)
    q = QUERY.format(where_extra=" ".join(clauses))
    cur = conn.cursor()
    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()
    return rows

# ── Punkt-Konvertierung ──────────────────────────────────────────────────────

def row_to_line(row, measurement_attr: str, extra_tags: dict) -> str | None:
    entity_id: str = row["entity_id"]           # z.B. "sensor.temperature_bedroom"
    domain, _, short_id = entity_id.partition(".")
    state: str = row["state"]
    ts_ns = int(float(row["last_updated_ts"]) * 1e9)

    attrs = {}
    if row["shared_attrs"]:
        try:
            attrs = json.loads(row["shared_attrs"])
        except (json.JSONDecodeError, TypeError):
            pass

    # ── Measurement bestimmen ────────────────────────────────────────────────
    if measurement_attr == "unit_of_measurement":
        uom = attrs.get("unit_of_measurement", "").strip()
        measurement = uom if uom else entity_id
    elif measurement_attr == "entity_id":
        measurement = entity_id
    else:
        measurement = attrs.get(measurement_attr) or entity_id

    # ── Tags ─────────────────────────────────────────────────────────────────
    tags = {
        "domain":    domain,
        "entity_id": short_id,
        **extra_tags,
    }

    # ── Fields ───────────────────────────────────────────────────────────────
    fields = {}

    # Primär-Wert: value (float) oder state (str)
    try:
        fields["value"] = float(state)
    except (ValueError, TypeError):
        fields["state"] = state

    # Attribute als weitere Fields (wie die Integration es tut)
    for attr_key, attr_val in attrs.items():
        if attr_key in IGNORE_ATTRIBUTES:
            continue
        if isinstance(attr_val, (list, dict)):
            # Komplexe Typen überspringen (HA tut das auch)
            continue
        fk = escape_field_key(attr_key)
        # HA hängt _str an, wenn es einen nicht-string Wert als String speichert
        if isinstance(attr_val, str):
            fields[attr_key] = attr_val
        elif isinstance(attr_val, (int, float, bool)):
            fields[attr_key] = attr_val
            fields[f"{attr_key}_str"] = str(attr_val)

    return build_line(measurement, tags, fields, ts_ns)

# ── State-File ───────────────────────────────────────────────────────────────

def load_last_ts() -> float | None:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return float(f.read().strip())
    return None

def save_last_ts(ts: float):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        f.write(str(ts))

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-ts",   type=float, help="Unix-Timestamp (Sekunden) ab dem exportiert wird")
    parser.add_argument("--to-ts",     type=float, help="Unix-Timestamp (Sekunden) bis zu dem exportiert wird")
    parser.add_argument("--full",      action="store_true", help="Ignoriert STATE_FILE, exportiert alles")
    parser.add_argument("--dry-run",   action="store_true", help="Kein Schreiben nach InfluxDB")
    parser.add_argument("--tag",       action="append", default=[], metavar="KEY=VALUE",
                        help="Zusätzliche Tags (z.B. --tag source=HA)")
    parser.add_argument("--measurement-attr", default=MEASUREMENT_ATTR,
                        choices=["unit_of_measurement", "entity_id"],
                        help="Wie HA das Measurement benennt (default: unit_of_measurement)")
    args = parser.parse_args()

    extra_tags = {}
    for t in args.tag:
        k, _, v = t.partition("=")
        if k and v:
            extra_tags[k] = v

    # Zeitfenster bestimmen
    from_ts = args.from_ts
    to_ts   = args.to_ts or datetime.now(timezone.utc).timestamp()

    if not args.full and from_ts is None:
        from_ts = load_last_ts()
        if from_ts:
            print(f"Inkrementell ab {datetime.fromtimestamp(from_ts, tz=timezone.utc).isoformat()}")
        else:
            print("Kein STATE_FILE gefunden — exportiere alles (Erstlauf)")

    rows = fetch_rows(HA_DB, from_ts, to_ts)
    print(f"{len(rows)} Zeilen aus HA-DB geladen")

    if not rows:
        print("Nichts zu exportieren.")
        return

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    batch, written, skipped = [], 0, 0
    max_ts = from_ts or 0.0

    for row in rows:
        line = row_to_line(row, args.measurement_attr, extra_tags)
        if line:
            batch.append(line)
            written += 1
        else:
            skipped += 1

        ts = float(row["last_updated_ts"])
        if ts > max_ts:
            max_ts = ts

        if len(batch) >= BATCH_SIZE:
            if not args.dry_run:
                write_api.write(bucket=INFLUX_BUCKET, record=batch)
            else:
                print(f"[dry-run] würde {len(batch)} Punkte schreiben")
            batch = []
            print(f"  {written} Punkte geschrieben...")

    if batch:
        if not args.dry_run:
            write_api.write(bucket=INFLUX_BUCKET, record=batch)
        else:
            print(f"[dry-run] würde {len(batch)} Punkte schreiben")

    if not args.dry_run and max_ts > 0:
        save_last_ts(max_ts)

    client.close()
    print(f"Fertig. Geschrieben: {written}, Übersprungen: {skipped}")
    print(f"Letzter exportierter Timestamp: {datetime.fromtimestamp(max_ts, tz=timezone.utc).isoformat()}")

if __name__ == "__main__":
    main()