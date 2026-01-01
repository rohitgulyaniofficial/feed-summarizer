#!/usr/bin/env python3
"""Test automatic SimHash migration on startup."""

import sqlite3
import tempfile
from pathlib import Path

from models.migrations import run_migrations
from utils import encode_int64


def test_simhash_migration_on_fresh_database():
    """Test that SimHash migration creates migration_log table and records completion."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Create minimal schema (items and feeds needed for summaries)
        cursor.execute(
            """
            CREATE TABLE feeds (
                id INTEGER PRIMARY KEY,
                slug TEXT UNIQUE,
                title TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                feed_id INTEGER,
                title TEXT,
                url TEXT UNIQUE,
                date INTEGER,
                FOREIGN KEY (feed_id) REFERENCES feeds(id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE summaries (
                id INTEGER PRIMARY KEY,
                summary_text TEXT,
                merge_simhash INTEGER,
                FOREIGN KEY (id) REFERENCES items(id)
            )
            """
        )
        conn.commit()

        # Insert test data
        cursor.execute("INSERT INTO feeds (id, slug, title) VALUES (1, 'test', 'Test Feed')")
        cursor.execute(
            "INSERT INTO items (id, feed_id, title, url, date) VALUES (1, 1, 'Test Article', 'http://test.com/1', 0)"
        )
        cursor.execute("INSERT INTO summaries (id, summary_text, merge_simhash) VALUES (1, 'Test summary text', NULL)")
        conn.commit()

        # Run migrations
        run_migrations(conn)

        # Verify migration_log table was created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='migration_log'")
        assert cursor.fetchone() is not None, "migration_log table should exist"

        # Verify migration was recorded
        cursor.execute("SELECT * FROM migration_log WHERE migration_name = 'recompute_merge_simhash_multilingual'")
        migration = cursor.fetchone()
        assert migration is not None, "SimHash migration should be recorded"
        assert migration["applied_date"] > 0, "Migration should have timestamp"
        assert "summaries" in migration["notes"], "Migration notes should mention summaries"

        # Verify SimHash was computed
        cursor.execute("SELECT merge_simhash FROM summaries WHERE id = 1")
        result = cursor.fetchone()
        assert result["merge_simhash"] is not None, "SimHash should be computed"

        conn.close()


def test_simhash_migration_idempotency():
    """Test that running migrations twice doesn't recompute SimHash."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Create minimal schema
        cursor.execute(
            """
            CREATE TABLE feeds (
                id INTEGER PRIMARY KEY,
                slug TEXT UNIQUE,
                title TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                feed_id INTEGER,
                title TEXT,
                url TEXT UNIQUE,
                date INTEGER,
                FOREIGN KEY (feed_id) REFERENCES feeds(id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE summaries (
                id INTEGER PRIMARY KEY,
                summary_text TEXT,
                merge_simhash INTEGER,
                FOREIGN KEY (id) REFERENCES items(id)
            )
            """
        )
        conn.commit()

        # Insert test data
        cursor.execute("INSERT INTO feeds (id, slug, title) VALUES (1, 'test', 'Test Feed')")
        cursor.execute(
            "INSERT INTO items (id, feed_id, title, url, date) VALUES (1, 1, 'Test Article', 'http://test.com/1', 0)"
        )
        cursor.execute("INSERT INTO summaries (id, summary_text, merge_simhash) VALUES (1, 'Test summary text', NULL)")
        conn.commit()

        # Run migrations first time
        run_migrations(conn)

        # Get the SimHash value
        cursor.execute("SELECT merge_simhash FROM summaries WHERE id = 1")
        first_hash = cursor.fetchone()["merge_simhash"]

        # Get the migration timestamp
        cursor.execute(
            "SELECT applied_date FROM migration_log WHERE migration_name = 'recompute_merge_simhash_multilingual'"
        )
        first_timestamp = cursor.fetchone()["applied_date"]

        # Run migrations second time
        run_migrations(conn)

        # Verify SimHash wasn't recomputed (same value)
        cursor.execute("SELECT merge_simhash FROM summaries WHERE id = 1")
        second_hash = cursor.fetchone()["merge_simhash"]
        assert first_hash == second_hash, "SimHash should not be recomputed on second run"

        # Verify migration timestamp didn't change
        cursor.execute(
            "SELECT applied_date FROM migration_log WHERE migration_name = 'recompute_merge_simhash_multilingual'"
        )
        second_timestamp = cursor.fetchone()["applied_date"]
        assert first_timestamp == second_timestamp, "Migration timestamp should not change"

        # Verify only one migration log entry exists
        cursor.execute("SELECT COUNT(*) as count FROM migration_log WHERE migration_name = 'recompute_merge_simhash_multilingual'")
        count = cursor.fetchone()["count"]
        assert count == 1, "Should only have one migration log entry"

        conn.close()


def test_simhash_migration_with_existing_hashes():
    """Test that migration only updates summaries that need updating."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Create minimal schema
        cursor.execute(
            """
            CREATE TABLE feeds (
                id INTEGER PRIMARY KEY,
                slug TEXT UNIQUE,
                title TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                feed_id INTEGER,
                title TEXT,
                url TEXT UNIQUE,
                date INTEGER,
                FOREIGN KEY (feed_id) REFERENCES feeds(id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE summaries (
                id INTEGER PRIMARY KEY,
                summary_text TEXT,
                merge_simhash INTEGER,
                FOREIGN KEY (id) REFERENCES items(id)
            )
            """
        )
        conn.commit()

        # Insert test data with pre-existing hashes
        cursor.execute("INSERT INTO feeds (id, slug, title) VALUES (1, 'test', 'Test Feed')")
        cursor.execute(
            "INSERT INTO items (id, feed_id, title, url, date) VALUES (1, 1, 'Test Article', 'http://test.com/1', 0)"
        )
        cursor.execute(
            "INSERT INTO items (id, feed_id, title, url, date) VALUES (2, 1, 'Another Article', 'http://test.com/2', 0)"
        )
        # First summary has a hash, second doesn't
        cursor.execute(
            f"INSERT INTO summaries (id, summary_text, merge_simhash) VALUES (1, 'Test summary', {encode_int64(12345)})"
        )
        cursor.execute("INSERT INTO summaries (id, summary_text, merge_simhash) VALUES (2, 'Another summary', NULL)")
        conn.commit()

        # Run migrations
        run_migrations(conn)

        # Verify both summaries have hashes now
        cursor.execute("SELECT merge_simhash FROM summaries WHERE id = 1")
        hash1 = cursor.fetchone()["merge_simhash"]
        cursor.execute("SELECT merge_simhash FROM summaries WHERE id = 2")
        hash2 = cursor.fetchone()["merge_simhash"]

        assert hash1 is not None, "First summary should have hash"
        assert hash2 is not None, "Second summary should have hash"
        assert hash1 != encode_int64(12345), "First summary hash should be recomputed"

        conn.close()
