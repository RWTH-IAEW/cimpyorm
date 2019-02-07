from cimpyorm.api import load, parse
import os
import pytest
import cimpyorm

from cimpyorm.backend import MariaDB


def test_parse_load(full_grid):
    try:
        cimpyorm.get_path("SCHEMAROOT")
    except KeyError:
        pytest.skip(f"Schemata not configured")
    path = "integration_test"
    session, m = parse(full_grid, MariaDB(path=path, host="mariadb"))
    session.close()
    session, m = load(MariaDB(path=path, host="mariadb"))
    session.close()
    MariaDB(path=path, host="mariadb").drop()


def test_parse_parse(full_grid):
    try:
        cimpyorm.get_path("SCHEMAROOT")
    except KeyError:
        pytest.skip(f"Schemata not configured")
    path = "integration_test"
    session, m = parse(full_grid, MariaDB(path=path, host="mariadb"))
    session.close()
    session, m = parse(full_grid, MariaDB(path=path, host="mariadb"))
    assert session.query(m.Terminal).first().ConductingEquipment
    session.close()
    MariaDB(path=path, host="mariadb").drop()
