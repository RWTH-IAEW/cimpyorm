#
#  Copyright (c) 2018 - 2019 Thomas Offergeld (offergeld@ifht.rwth-aachen.de)
#  Institute for High Voltage Technology
#  RWTH Aachen University
#
#  This module is part of CIMPy.
#
#  CIMPy is licensed under the BSD-3-Clause license.
#  For further information see LICENSE in the project's root directory.
#
import os
from pathlib import Path

import requests
import toml

from cimpyorm import get_path


def test_pypi_version():
    pypi_meta = requests.get("https://pypi.python.org/pypi/cimpyorm/json").json()
    pypi_releases = pypi_meta["releases"].keys()
    path = Path(get_path("PACKAGEROOT")).parent
    toml_meta = toml.loads(open(os.path.join(path, "pyproject.toml")).read())
    toml_version = toml_meta["tool"]["poetry"]["version"]
    assert toml_version in pypi_releases
