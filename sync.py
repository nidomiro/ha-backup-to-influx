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
Backup:       --backup <backup.tar> liest die DB direkt aus einem HA-Backup
              (verschlüsselt: Schlüssel in HA_BACKUP_PASSWORD).
"""

import argparse
import atexit
import contextlib
import json
import math
import os
import re
import shutil
import sqlite3
import sys
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

# ── Konfiguration ────────────────────────────────────────────────────────────

INFLUX_URL    = os.getenv("INFLUX_URL",    "https://influxdb.hl443.de")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "d15e4819fb951381")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "homeassistant_2")
HA_DB         = os.getenv("HA_DB",         "./home-assistant_v2.db")
STATE_FILE    = os.getenv("STATE_FILE",    "./last_export_ts.txt")
CONFLICTS_FILE = os.getenv("CONFLICTS_FILE", "./conflicts.log")

# Entspricht measurement_attr: unit_of_measurement (HA-Default)
# Auf "entity_id" setzen wenn du measurement_attr: entity_id verwendest
MEASUREMENT_ATTR = os.getenv("MEASUREMENT_ATTR", "unit_of_measurement")

BATCH_SIZE = 5000

# ── HA-Backup (.tar) ─────────────────────────────────────────────────────────

BACKUP_PASSWORD = os.getenv("HA_BACKUP_PASSWORD", "")
BACKUP_INNER = "homeassistant.tar.gz"
SECURETAR_MAGIC = b"SecureTar"
DB_NAME = "home-assistant_v2.db"

@contextlib.contextmanager
def _open_inner_tar(backup_path: str):
    """Öffnet homeassistant.tar.gz aus dem Backup als Stream-Tarfile (r|gz).

    Verschlüsselte Backups (SecureTar) werden on-the-fly entschlüsselt.
    """
    with tarfile.open(backup_path, "r:") as outer:
        try:
            inner = outer.getmember(BACKUP_INNER)
        except KeyError:
            sys.exit(f"FEHLER: {BACKUP_INNER} nicht in {backup_path} "
                     f"(kein Core-Backup?). Enthalten: {outer.getnames()}")
        raw = outer.extractfile(inner)
        if raw is None:
            sys.exit(f"FEHLER: {BACKUP_INNER} in {backup_path} ist keine reguläre Datei.")
        encrypted = raw.read(len(SECURETAR_MAGIC)) == SECURETAR_MAGIC
        raw.seek(0)
        print(f"Lese {DB_NAME} aus {backup_path} "
              f"({'verschlüsselt' if encrypted else 'unverschlüsselt'}) …")

        if not encrypted:
            with tarfile.open(fileobj=raw, mode="r|gz") as inner_tar:
                yield inner_tar
            return

        if not BACKUP_PASSWORD:
            sys.exit("FEHLER: Backup ist verschlüsselt – HA_BACKUP_PASSWORD setzen "
                     "(Backup-Schlüssel aus den HA-Backup-Einstellungen).")
        try:
            from securetar import SecureTarArchive, SecureTarError
        except ImportError:
            sys.exit("FEHLER: 'securetar' fehlt (pip install -r requirements.txt)")
        try:
            with SecureTarArchive(Path(backup_path), "r", password=BACKUP_PASSWORD) as archive:
                with archive.extract_tar(archive.tar.getmember(BACKUP_INNER)) as reader, \
                        tarfile.open(fileobj=cast(Any, reader), mode="r|gz") as inner_tar:
                    yield inner_tar
        except SecureTarError as e:
            sys.exit(f"FEHLER: Entschlüsselung fehlgeschlagen (Passwort prüfen): {e}")

def extract_db_from_backup(backup_path: str, workdir: str) -> str:
    """Streamt data/home-assistant_v2.db (+ -wal) aus einem HA-Backup nach workdir.

    Das äußere Tar ist seekbar (nur homeassistant.tar.gz wird gelesen, Add-ons
    werden übersprungen). Das innere Archiv wird gestreamt – nie komplett im RAM.
    """
    wanted = {f"data/{DB_NAME}": DB_NAME, f"data/{DB_NAME}-wal": f"{DB_NAME}-wal"}
    found = set()
    with _open_inner_tar(backup_path) as inner_tar:
        for member in inner_tar:
            name = member.name.removeprefix("./")
            if name not in wanted or not member.isfile():
                continue
            print(f"  extrahiere {name} ({member.size / 1e9:.1f} GB)")
            with open(os.path.join(workdir, wanted[name]), "wb") as out:
                shutil.copyfileobj(inner_tar.extractfile(member), out, length=8 << 20)
            found.add(wanted[name])
    if DB_NAME not in found:
        sys.exit(f"FEHLER: data/{DB_NAME} nicht in {BACKUP_INNER} gefunden.")
    return os.path.join(workdir, DB_NAME)

# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

# 1:1 wie homeassistant/components/influxdb (default: include_uom=False, include_dc=False)
IGNORE_ATTRIBUTES = {"unit_of_measurement", "device_class"}

# Aus homeassistant/components/influxdb/const.py
RE_DIGIT_TAIL = re.compile(r"^[^\.]*\d+\.?\d+[^\.]*$")
RE_DECIMAL = re.compile(r"[^\d.]+")

# Aus homeassistant/helpers/state.py:state_as_number
_STATE_ON_LIKE  = {"on", "locked", "above_horizon", "open", "home"}
_STATE_OFF_LIKE = {"off", "unlocked", "unknown", "below_horizon", "closed", "not_home"}

def _state_as_number(state: str) -> float:
    """Replikat von homeassistant.helpers.state.state_as_number."""
    if state in _STATE_ON_LIKE:
        return 1.0
    if state in _STATE_OFF_LIKE:
        return 0.0
    return float(state)  # ValueError wenn nicht parsebar

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

def iter_rows(db_path: str, from_ts: float | None, to_ts: float | None):
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
    try:
        for row in cur:
            yield row
    finally:
        cur.close()
        conn.close()

# ── Punkt-Konvertierung ──────────────────────────────────────────────────────

def row_to_point(row, measurement_attr: str, extra_tags: dict) -> Point | None:
    """1:1-Port von homeassistant/components/influxdb event_to_json.
    Siehe https://github.com/home-assistant/core/blob/dev/homeassistant/components/influxdb/__init__.py
    """
    entity_id: str = row["entity_id"]           # z.B. "sensor.temperature_bedroom"
    domain, _, short_id = entity_id.partition(".")
    state = row["state"]
    ts_ns = int(float(row["last_updated_ts"]) * 1e9)

    attrs = {}
    if row["shared_attrs"]:
        try:
            attrs = json.loads(row["shared_attrs"])
        except (json.JSONDecodeError, TypeError):
            pass

    # ── Measurement bestimmen (analog HA: unit_of_measurement → entity_id-Fallback)
    if measurement_attr == "unit_of_measurement":
        uom = attrs.get("unit_of_measurement")
        measurement = uom if isinstance(uom, str) and uom.strip() else entity_id
    elif measurement_attr == "entity_id":
        measurement = entity_id
    else:
        measurement = attrs.get(measurement_attr) or entity_id

    # ── State → fields["value"] / fields["state"] (HA event_to_json) ─────────
    fields: dict[str, float | str] = {}
    try:
        fields["value"] = float(state)
    except (ValueError, TypeError):
        try:
            fields["value"] = float(_state_as_number(state))
            fields["state"] = state
        except (ValueError, TypeError):
            if state is not None:
                fields["state"] = state

    # ── Attribute → fields (HA event_to_json) ────────────────────────────────
    for key, value in attrs.items():
        if key in IGNORE_ATTRIBUTES:
            continue
        # Kollision: wenn Key schon belegt (z.B. Attribut "state" oder "value"),
        # hängt HA einen Underscore an.
        if key in fields:
            key = f"{key}_"

        try:
            fields[key] = float(value)
        except (ValueError, TypeError):
            new_key = f"{key}_str"
            new_value = str(value)
            fields[new_key] = new_value
            if RE_DIGIT_TAIL.match(new_value):
                try:
                    fields[key] = float(RE_DECIMAL.sub("", new_value))
                except ValueError:
                    pass

        # NaN/Infinity raus (HA macht das auch)
        v = fields.get(key)
        if isinstance(v, float) and not math.isfinite(v):
            del fields[key]

    if not fields:
        return None

    point = Point(measurement).time(ts_ns, write_precision=WritePrecision.NS)
    point.tag("domain", domain)
    point.tag("entity_id", short_id)
    for k, v in extra_tags.items():
        point.tag(k, v)
    for k, v in fields.items():
        point.field(k, v)
    return point

# ── State-File ───────────────────────────────────────────────────────────────

def load_last_ts() -> float | None:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return float(f.read().strip())
    return None

def save_last_ts(ts: float):
    parent = os.path.dirname(STATE_FILE)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        f.write(str(ts))

# ── InfluxDB-Check ───────────────────────────────────────────────────────────

def check_influx(client: InfluxDBClient, bucket: str):
    """Verifiziert Erreichbarkeit, Auth und Bucket-Zugriff. Beendet bei Fehler."""
    try:
        if not client.ping():
            print(f"FEHLER: InfluxDB unter {INFLUX_URL} nicht erreichbar (ping fehlgeschlagen).", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"FEHLER: InfluxDB unter {INFLUX_URL} nicht erreichbar: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        b = client.buckets_api().find_bucket_by_name(bucket)
    except Exception as e:
        print(f"FEHLER: Bucket-Lookup fehlgeschlagen (Token/Org prüfen): {e}", file=sys.stderr)
        sys.exit(1)

    if b is None:
        print(f"FEHLER: Bucket '{bucket}' nicht gefunden in Org '{INFLUX_ORG}'.", file=sys.stderr)
        sys.exit(1)

    print(f"InfluxDB OK: {INFLUX_URL}, Bucket '{bucket}' erreichbar.")

# ── Batch-Flush mit Konflikt-Logging ─────────────────────────────────────────

def flush_batch(write_api, bucket: str, batch: list[tuple[Point, float]],
                dry_run: bool, conflicts_path: str) -> tuple[int, float | None]:
    """Schreibt batch nach Influx. Bei Fehler: Batch als Line-Protocol nach
    conflicts_path schreiben, Fehler ausgeben und Programm beenden.

    Returns (written_count, batch_max_ts_or_None).
    """
    if not batch:
        return 0, None
    batch_max_ts = max(ts for _, ts in batch)

    if dry_run:
        print(f"[dry-run] würde {len(batch)} Punkte schreiben")
        return len(batch), batch_max_ts

    try:
        write_api.write(bucket=bucket, record=[p for p, _ in batch],
                        write_precision=WritePrecision.NS)
        return len(batch), batch_max_ts
    except ApiException as e:
        body = (e.body or "").strip()
        ts_min = min(ts for _, ts in batch)
        ts_max = batch_max_ts
        print(f"FEHLER: Batch-Write fehlgeschlagen ({e.status}): {body}", file=sys.stderr)
        print(f"  Batch: {len(batch)} Punkte, ts={ts_min} .. {ts_max}", file=sys.stderr)
        with open(conflicts_path, "a") as log:
            log.write(f"# {datetime.now(timezone.utc).isoformat()} batch failure status={e.status} body={body}\n")
            for point, _ts in batch:
                log.write(point.to_line_protocol() + "\n")
        print(f"  Batch-Daten ({len(batch)} Zeilen) nach {conflicts_path} geschrieben.", file=sys.stderr)
        print("Abbruch. STATE_FILE steht auf dem letzten erfolgreich geschriebenen Timestamp; "
              "nach dem Bereinigen einfach neu starten.", file=sys.stderr)
        sys.exit(1)

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-ts",   type=float, help="Unix-Timestamp (Sekunden) ab dem exportiert wird")
    parser.add_argument("--to-ts",     type=float, help="Unix-Timestamp (Sekunden) bis zu dem exportiert wird")
    parser.add_argument("--full",      action="store_true", help="Ignoriert STATE_FILE, exportiert alles")
    parser.add_argument("--dry-run",   action="store_true", help="Kein Schreiben nach InfluxDB")
    parser.add_argument("--backup",    metavar="TAR",
                        help="HA-Backup (.tar, ggf. verschlüsselt, Passwort in HA_BACKUP_PASSWORD); "
                             "home-assistant_v2.db wird daraus in ein Temp-Verzeichnis extrahiert")
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

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    if not args.dry_run:
        check_influx(client, INFLUX_BUCKET)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    batch: list[tuple[Point, float]] = []
    written, skipped = 0, 0
    last_seen_ts = from_ts or 0.0
    last_written_ts = from_ts or 0.0

    def do_flush():
        nonlocal written, last_written_ts
        ok, _ = flush_batch(write_api, INFLUX_BUCKET, batch, args.dry_run, CONFLICTS_FILE)
        written += ok
        # Erfolg: alle Rows bis last_seen_ts sind durch (auch übersprungene).
        if last_seen_ts > last_written_ts:
            last_written_ts = last_seen_ts
            if not args.dry_run:
                save_last_ts(last_written_ts)
        batch.clear()
        print(f"  {written} geschrieben, {skipped} übersprungen")

    db_path = HA_DB
    if args.backup:
        tmp = tempfile.TemporaryDirectory(prefix=".ha-extract-", dir=".")
        atexit.register(tmp.cleanup)
        db_path = extract_db_from_backup(args.backup, tmp.name)

    for row in iter_rows(db_path, from_ts, to_ts):
        point = row_to_point(row, args.measurement_attr, extra_tags)
        ts = float(row["last_updated_ts"])
        last_seen_ts = ts  # iter_rows liefert nach ts sortiert
        if point is not None:
            batch.append((point, ts))
        else:
            skipped += 1

        if len(batch) >= BATCH_SIZE:
            do_flush()

    if batch:
        do_flush()

    client.close()
    if written == 0 and skipped == 0:
        print("Nichts zu exportieren.")
        return
    print(f"Fertig. Geschrieben: {written}, Übersprungen: {skipped}")
    if last_written_ts > 0:
        last_dt = datetime.fromtimestamp(last_written_ts, tz=timezone.utc)
        print(f"Letzter geschriebener Timestamp: {last_dt.isoformat()}")

if __name__ == "__main__":
    main()
