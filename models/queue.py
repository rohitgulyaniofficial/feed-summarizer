#!/usr/bin/env python3
"""Async database queue wrapper around a single SQLite connection."""

from __future__ import annotations

from asyncio import CancelledError, Event, Queue, TimeoutError, create_task, wait_for
from os import path
from sqlite3 import Row, connect
from typing import Any, Dict
from uuid import uuid4

from config import get_logger
from services.telemetry import trace_span

from models.ops_bulletins import BulletinsOpsMixin
from models.ops_feeds_items import FeedsItemsOpsMixin
from models.ops_maintenance import MaintenanceOpsMixin
from models.ops_status import StatusOpsMixin
from models.ops_summaries import SummariesOpsMixin
from models.schema import initialize_database

logger = get_logger("models")


class DatabaseQueue(
    FeedsItemsOpsMixin,
    SummariesOpsMixin,
    BulletinsOpsMixin,
    MaintenanceOpsMixin,
    StatusOpsMixin,
):
    """A queue for database operations to ensure thread safety."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.queue: Queue = Queue()
        self.results: Dict[str, Dict] = {}
        self.events: Dict[str, Event] = {}
        self.conn = None
        self.running = False
        self.worker_task = None

    async def start(self) -> None:
        """Start the database worker."""
        if self.running:
            return
        self.running = True
        self.worker_task = create_task(self._worker())
        logger.info("Database worker started")

    async def stop(self) -> None:
        """Stop the database worker."""
        if not self.running:
            return
        self.running = False
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except CancelledError:
                pass

        if self.conn:
            self.conn.close()
            self.conn = None

        for event in self.events.values():
            event.set()
        self.events.clear()
        self.results.clear()

        logger.info("Database worker stopped")

    async def _worker(self) -> None:
        """Worker coroutine processing database operations."""
        db_exists = path.isfile(self.db_path)
        if not db_exists:
            logger.info(
                f"Database file {self.db_path} does not exist. A new database will be created."
            )
        else:
            logger.info(f"Using existing database at {self.db_path}")

        self.conn = connect(self.db_path)
        self.conn.row_factory = Row

        try:
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.warning(f"Could not enforce WAL mode: {e}")

        initialize_database(self.conn)

        while self.running:
            try:
                try:
                    operation_id, operation_name, params = await wait_for(
                        self.queue.get(), timeout=1.0
                    )
                except TimeoutError:
                    continue

                try:
                    if hasattr(self, operation_name):
                        method = getattr(self, operation_name)
                        result = method(**params)
                        self.results[operation_id] = {"result": result}
                    else:
                        self.results[operation_id] = {
                            "error": f"Unknown operation: {operation_name}"
                        }
                except Exception as e:
                    logger.error(f"Database operation error in {operation_name}: {e}")
                    self.results[operation_id] = {"error": str(e)}
                finally:
                    if operation_id in self.events:
                        self.events[operation_id].set()
                    self.queue.task_done()
            except CancelledError:
                logger.info("Database worker cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in database worker: {e}")

    @trace_span(
        "db.execute",
        tracer_name="db",
        static_attrs={"db.system": "sqlite"},
        attr_from_args=lambda self, operation_name, **params: {
            "db.operation": operation_name,
            "db.params.keys": ",".join(sorted(params.keys())) if params else "",
        },
    )
    async def execute(self, operation_name: str, **params) -> Any:
        """Execute a database operation."""
        operation_id = str(uuid4())
        event = Event()
        self.events[operation_id] = event
        try:
            await self.queue.put((operation_id, operation_name, params))
            await event.wait()
            result = self.results.pop(operation_id)
            if "error" in result:
                raise Exception(result["error"])
            return result["result"]
        finally:
            self.events.pop(operation_id, None)
