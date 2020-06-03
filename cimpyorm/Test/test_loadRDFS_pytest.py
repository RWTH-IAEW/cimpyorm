#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

import os
import pytest
from cimpyorm.auxiliary import get_path, find_rdfs_path


@pytest.mark.parametrize("Version", [(16)])
def test_find_valid_rdfs_version(Version):
    try:
        os.path.isdir(get_path("SCHEMAROOT"))
    except KeyError:
        pytest.skip(f"Schema folder not configured")
    version = f"{Version}"
    rdfs_path = find_rdfs_path(version)
    assert os.path.isdir(rdfs_path) and os.listdir(rdfs_path)


@pytest.mark.parametrize("Version", [(9), (153), ("foo"), ("ba")])
def test_find_invalid_rdfs_version(Version):
    try:
        os.path.isdir(get_path("SCHEMAROOT"))
    except KeyError:
        pytest.skip(f"Schema folder not configured")
    with pytest.raises((ValueError, NotImplementedError)) as ex_info:
        version = f"{Version}"
        find_rdfs_path(version)
    print(ex_info)
