import sqlite3
import tarfile
from pathlib import Path

import pytest
from securetar import SecureTarArchive

import sync

PASSWORD = "pw-1234"


@pytest.fixture
def source_db(tmp_path):
    path = tmp_path / "src.db"
    conn = sqlite3.connect(path)
    conn.execute("create table t(a)")
    conn.execute("insert into t values (42)")
    conn.commit()
    conn.close()
    return path


def _extract(backup: Path, tmp_path: Path) -> list:
    out = tmp_path / "out"
    out.mkdir()
    db = sync.extract_db_from_backup(str(backup), str(out))
    return sqlite3.connect(db).execute("select a from t").fetchall()


def test_encrypted_backup(source_db, tmp_path, monkeypatch):
    monkeypatch.setattr(sync, "BACKUP_PASSWORD", PASSWORD)
    backup = tmp_path / "enc.tar"
    with SecureTarArchive(backup, "w", password=PASSWORD) as archive:
        with archive.create_tar("homeassistant.tar.gz") as tar:
            tar.add(source_db, arcname="data/other")
            tar.add(source_db, arcname="data/home-assistant_v2.db")
    assert _extract(backup, tmp_path) == [(42,)]


def test_plain_backup_with_dot_prefix(source_db, tmp_path):
    inner = tmp_path / "inner.tar.gz"
    with tarfile.open(inner, "w:gz") as tar:
        tar.add(source_db, arcname="./data/home-assistant_v2.db")
    backup = tmp_path / "plain.tar"
    with tarfile.open(backup, "w") as tar:
        tar.add(inner, arcname="homeassistant.tar.gz")
    assert _extract(backup, tmp_path) == [(42,)]


def test_wrong_password_exits(source_db, tmp_path, monkeypatch):
    backup = tmp_path / "enc.tar"
    with SecureTarArchive(backup, "w", password=PASSWORD) as archive:
        with archive.create_tar("homeassistant.tar.gz") as tar:
            tar.add(source_db, arcname="data/home-assistant_v2.db")
    monkeypatch.setattr(sync, "BACKUP_PASSWORD", "wrong")
    with pytest.raises(SystemExit, match="Passwort"):
        _extract(backup, tmp_path)


def test_encrypted_without_password_exits(source_db, tmp_path, monkeypatch):
    backup = tmp_path / "enc.tar"
    with SecureTarArchive(backup, "w", password=PASSWORD) as archive:
        with archive.create_tar("homeassistant.tar.gz") as tar:
            tar.add(source_db, arcname="data/home-assistant_v2.db")
    monkeypatch.setattr(sync, "BACKUP_PASSWORD", "")
    with pytest.raises(SystemExit, match="HA_BACKUP_PASSWORD"):
        _extract(backup, tmp_path)


def test_missing_db_exits(source_db, tmp_path):
    inner = tmp_path / "inner.tar.gz"
    with tarfile.open(inner, "w:gz") as tar:
        tar.add(source_db, arcname="data/other.db")
    backup = tmp_path / "plain.tar"
    with tarfile.open(backup, "w") as tar:
        tar.add(inner, arcname="homeassistant.tar.gz")
    with pytest.raises(SystemExit, match="nicht in"):
        _extract(backup, tmp_path)
