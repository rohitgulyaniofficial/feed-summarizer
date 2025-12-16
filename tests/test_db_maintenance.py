import os
import sqlite3

import pytest

from models import DatabaseQueue


@pytest.mark.asyncio
async def test_db_maintenance_checkpoints_and_can_vacuum(tmp_path):
    db_path = tmp_path / "test.db"

    # Create a small DB in WAL mode and generate a non-trivial WAL.
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    for i in range(2000):
        cur.execute("INSERT INTO t (v) VALUES (?)", (f"value-{i}",))
    conn.commit()
    cur.close()
    conn.close()

    wal_path = db_path.with_name(db_path.name + "-wal")
    # SQLite may auto-checkpoint and remove the WAL on close, depending on version/settings.
    pre_size = os.path.getsize(wal_path) if wal_path.exists() else None

    dbq = DatabaseQueue(str(db_path))
    await dbq.start()
    try:
        res = await dbq.execute(
            "perform_maintenance",
            checkpoint_mode="TRUNCATE",
            vacuum=True,
            optimize=True,
            busy_timeout_ms=1000,
        )
        assert isinstance(res, dict)
    finally:
        await dbq.stop()

    # After TRUNCATE checkpoint + close, WAL should be gone or empty.
    if wal_path.exists():
        assert os.path.getsize(wal_path) == 0

    # If there was a WAL before maintenance, it should not have grown.
    if pre_size is not None and wal_path.exists():
        assert os.path.getsize(wal_path) <= pre_size

    # DB remains readable.
    conn2 = sqlite3.connect(db_path)
    cur2 = conn2.cursor()
    cur2.execute("SELECT COUNT(*) FROM t")
    count = cur2.fetchone()[0]
    cur2.close()
    conn2.close()
    assert count == 2000
