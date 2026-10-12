# ha-influx-backfill

Backfill Home Assistant history into InfluxDB 2, either from the live SQLite database or straight from a Home Assistant backup `.tar`.

Points are written with the same schema as the native HA InfluxDB integration (`api_version: 2`):

- **Measurement:** `unit_of_measurement` if present, otherwise `entity_id` (see `--measurement-attr`)
- **Tags:** `domain`, `entity_id` (without domain prefix), plus any `--tag key=value`
- **Fields:** `value` (float) or `state` (string), plus state attributes
- **Timestamp:** `last_updated_ts`, UTC

Writes are idempotent (InfluxDB overwrites identical measurement/tags/timestamp) and incremental: the last exported timestamp is stored in `last_export_ts.txt`.

## Setup

Requires [mise](https://mise.jdx.dev). It installs Python, creates `.venv` and installs the dependencies.

```sh
cp .env.example .env   # then fill in the values
```

| Variable             | Purpose                                                          | Default                    |
| -------------------- | ---------------------------------------------------------------- | -------------------------- |
| `INFLUX_TOKEN`       | InfluxDB API token (required unless `--dry-run`)                 |                            |
| `INFLUX_URL`         | InfluxDB URL                                                     | `https://influxdb.hl443.de`|
| `INFLUX_ORG`         | InfluxDB org                                                     | `d15e4819fb951381`         |
| `INFLUX_BUCKET`      | Target bucket                                                    | `homeassistant_2`          |
| `HA_DB`              | Path to the SQLite DB                                            | `./home-assistant_v2.db`   |
| `HA_BACKUP_PASSWORD` | Backup encryption key, only for encrypted backups                |                            |
| `STATE_FILE`         | Last exported timestamp                                          | `./last_export_ts.txt`     |
| `CONFLICTS_FILE`     | Rejected batches are appended here as line protocol              | `./conflicts.log`          |
| `MEASUREMENT_ATTR`   | `unit_of_measurement` or `entity_id`                             | `unit_of_measurement`      |

The defaults for URL and org are specific to the author's setup; override them in `.env`.

`.env` is loaded by mise and is gitignored.

## Usage

```sh
mise run sync                     # incremental export from HA_DB
mise run dry-run                  # same, but nothing is written to InfluxDB
mise run full                     # ignore the state file, export everything
mise run backup <backup.tar>      # export from an HA backup
```

Extra arguments go through to `sync.py`, e.g. `mise run sync -- --tag source=HA --from-ts 1760000000`.
Run `python sync.py --help` for all options.

### From a backup

`--backup` reads `data/home-assistant_v2.db` (and `-wal` if present) from the backup's `homeassistant.tar.gz` and extracts it into a temporary `.ha-extract-*/` directory, which is removed on exit.

- Encrypted backups (SecureTar) are decrypted on the fly. Set `HA_BACKUP_PASSWORD` to the backup key from the HA backup settings.
- The inner archive is streamed, so memory use stays flat, but the whole archive has to be read once to find the DB. For a ~20 GB backup that takes a while.
- You need free disk space for the extracted DB (around 13 GB for a large instance).
- The state file still applies. A backup older than `last_export_ts.txt` exports nothing; use `--full` or `--from-ts`.

## Development

```sh
mise run check    # ruff + mypy + pytest, same as CI
```

## License

[Unlicense](LICENSE): public domain, do whatever you want.
