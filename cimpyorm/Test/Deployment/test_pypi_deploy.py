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
from pathlib import Path

import requests
import toml

from cimpyorm.auxiliary import get_path


def test_pypi_version():
    pypi_meta = requests.get("https://pypi.python.org/pypi/cimpyorm/json").json()
    pypi_releases = pypi_meta["releases"].keys()
    path = Path(get_path("PACKAGEROOT")).parent
    toml_meta = toml.loads(open(os.path.join(path, "pyproject.toml")).read())
    toml_version = toml_meta["tool"]["poetry"]["version"]
    assert toml_version in pypi_releases
