"""A guard on the sqlite shim, not on the schema.

The tests run the real SQL against a translated copy of sql/001_schema.sql.
If a column is added to Postgres using a type the translation does not know,
every other test keeps passing against a table that no longer matches. These
two fail instead.
"""

import re

from tests.pgshim import SCHEMA_PATH, connect, translate_ddl

DECLARED_TABLES = {"ingest_objects", "readings", "rejected_rows"}


def declared_columns(table: str) -> set[str]:
    ddl = SCHEMA_PATH.read_text()
    block = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\);", ddl, re.S)
    assert block, f"{table} is not declared in {SCHEMA_PATH.name}"
    columns = set()
    for line in block.group(1).strip().splitlines():
        first = line.strip().split()[0]
        if first.upper() not in {"PRIMARY", "UNIQUE", "CONSTRAINT", "FOREIGN", "CHECK"}:
            columns.add(first)
    return columns


def test_every_declared_table_survives_the_translation():
    conn = connect()
    try:
        rows = conn.query("SELECT name FROM sqlite_master WHERE type='table'")
        assert {row[0] for row in rows} >= DECLARED_TABLES
    finally:
        conn.close()


def test_every_declared_column_exists_after_translation():
    conn = connect()
    try:
        for table in sorted(DECLARED_TABLES):
            actual = {row[1] for row in conn.query(f"PRAGMA table_info({table})")}
            assert declared_columns(table) == actual, table
    finally:
        conn.close()


def test_the_translation_leaves_postgres_only_types_behind():
    translated = translate_ddl(SCHEMA_PATH.read_text())
    for leftover in ("TIMESTAMPTZ", "BIGSERIAL", "NUMERIC("):
        assert leftover not in translated.upper()
