#!/usr/bin/env python3
"""Schema initialization and migrations.

Kept separate from the queue implementation for clarity and easier testing.
"""

from __future__ import annotations

from os import R_OK, access, path
from typing import Any

from config import config
from config import get_logger
from models.migrations import run_migrations

logger = get_logger("models")


def initialize_database(conn: Any) -> None:
    """Initialize the database with the defined schema from SQL file."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='feeds'"
        )
        feeds_table_exists = cursor.fetchone() is not None

        if not feeds_table_exists:
            logger.info("Database is new or empty. Initializing schema.")
            schema_sql = _read_schema_file()
            cursor.executescript(schema_sql)
            conn.commit()
            logger.info("Database schema initialized successfully")
        else:
            logger.info("Database already exists with proper schema")
            run_migrations(conn)
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise
    finally:
        cursor.close()


def _read_schema_file() -> str:
    """Read the schema from the configured SQL file."""
    schema_path = config.SCHEMA_FILE_PATH
    try:
        if not path.isfile(schema_path):
            raise FileNotFoundError(f"Schema file not found at {schema_path}")
        if not access(schema_path, R_OK):
            raise PermissionError(f"No read permission for schema file at {schema_path}")

        file_size = path.getsize(schema_path)
        max_size = config.SCHEMA_FILE_SIZE_LIMIT_MB * 1024 * 1024
        if file_size > max_size:
            raise ValueError(
                f"Schema file too large: {file_size} bytes (limit: {max_size} bytes)"
            )

        with open(schema_path, "r") as handle:
            return handle.read()
    except Exception as e:
        logger.error(f"Error reading schema file: {e}")
        raise
