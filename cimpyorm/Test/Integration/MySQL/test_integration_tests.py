from cimpyorm.api import load, parse
import pytest
import cimpyorm

from cimpyorm.backend import MySQL


def test_parse_load(full_grid):
    try:
        cimpyorm.get_path("SCHEMAROOT")
    except KeyError:
        pytest.skip(f"Schemata not configured")
    path = "integration_test"
    session, m = parse(full_grid, MySQL(path=path, host="mysql"))
    session.close()
    session, m = load(MySQL(path=path, host="mysql"))
    session.close()
    MySQL(path=path, host="mysql").drop()


def test_parse_parse(full_grid):
    try:
        cimpyorm.get_path("SCHEMAROOT")
    except KeyError:
        pytest.skip(f"Schemata not configured")
    path = "integration_test"
    session, m = parse(full_grid, MySQL(path=path, host="mysql"))
    session.close()
    session, m = parse(full_grid, MySQL(path=path, host="mysql"))
    assert session.query(m.Terminal).first().ConductingEquipment
    session.close()
    MySQL(path=path, host="mysql").drop()
