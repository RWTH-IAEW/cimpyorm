import cimpyorm.auxiliary
from cimpyorm.api import load, parse
import os
import pytest
import cimpyorm

from cimpyorm.backends import SQLite, InMemory


def test_parse_inmemory(full_grid):
    try:
        cimpyorm.auxiliary.get_path("SCHEMAROOT")
    except KeyError:
        pytest.skip(f"Schemata not configured")
    session, m = parse(full_grid, InMemory())
    session.close()


def test_parse_load(full_grid):
    try:
        cimpyorm.auxiliary.get_path("SCHEMAROOT")
    except KeyError:
        pytest.skip(f"Schemata not configured")
    path = os.path.join(full_grid, ".integration_test.db")
    session, m = parse(full_grid, SQLite(path=path))
    session.close()
    session, m = load(path)
    session.close()
    os.remove(path)


def test_parse_parse(full_grid):
    try:
        cimpyorm.auxiliary.get_path("SCHEMAROOT")
    except KeyError:
        pytest.skip(f"Schemata not configured")
    path = os.path.join(full_grid, ".integration_test.db")
    session, m = parse(full_grid, SQLite(path=path))
    session.close()
    session, m = parse(full_grid, SQLite(path=path))
    assert session.query(m.Terminal).first().ConductingEquipment
    session.close()
    os.remove(path)
