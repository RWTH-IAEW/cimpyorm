#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

import cimpyorm.auxiliary
from cimpyorm.api import load, parse
import pytest
import cimpyorm

from cimpyorm.backends import MySQL


def test_parse_load(full_grid):
    try:
        cimpyorm.auxiliary.get_path("SCHEMAROOT")
    except KeyError:
        pytest.skip(f"Schemata not configured")
    path = "integration_test"
    session, m = parse(full_grid, MySQL(path=path, host="localhost"))
    session.close()
    session, m = load(MySQL(path=path, host="localhost"))
    session.close()
    MySQL(path=path, host="localhost").drop()


def test_parse_parse(full_grid):
    try:
        cimpyorm.auxiliary.get_path("SCHEMAROOT")
    except KeyError:
        pytest.skip(f"Schemata not configured")
    path = "integration_test"
    session, m = parse(full_grid, MySQL(path=path, host="localhost"))
    session.close()
    session, m = parse(full_grid, MySQL(path=path, host="localhost"))
    assert session.query(m.Terminal).first().ConductingEquipment
    session.close()
    MySQL(path=path, host="localhost").drop()
