#
#  Copyright (c) 2018 - 2018 Thomas Offergeld (offergeld@ifht.rwth-aachen.de)
#  Institute for High Voltage Technology
#  RWTH Aachen University
#
#  This module is part of cimpyorm.
#
#  cimpyorm is licensed under the BSD-3-Clause license.
#  For further information see LICENSE in the project's root directory.
#
"""
cimpyorm creates ORM representations of CIM datasets.
This module sets up and provides configuration and imports.
"""
# pylint: disable=ungrouped-imports

import os

from cimpyorm.auxiliary import log, get_path, _CONFIGPATH, CONFIG, _TESTROOT, _PACKAGEROOT

if not os.path.isfile(_CONFIGPATH):
    with open(_CONFIGPATH, "w+") as f:
        # Update config.ini
        CONFIG.write(f)

try:
    import pytest

    def test_all(runslow=False):
        if runslow:
            pytest.main([os.path.join(_TESTROOT), "--runslow"])
        else:
            pytest.main([os.path.join(_TESTROOT)])
except ModuleNotFoundError:
    pass


try:
    # See if we already know a schemaroot
    CONFIG["Paths"]["SCHEMAROOT"] = get_path("SCHEMAROOT")
    if not os.path.isdir(CONFIG["Paths"]["SCHEMAROOT"]):
        # Is schemaroot an actual directory?
        log.warning(f"Invalid schema path in configuration.")
        raise NotADirectoryError
except (KeyError, NotADirectoryError):
    if os.path.isdir(os.path.join(_PACKAGEROOT, "res", "schemata")):
        # Look in the default path
        CONFIG["Paths"]["SCHEMAROOT"] = os.path.join(_PACKAGEROOT, "res", "schemata")
        log.info(f"Found schemata in default location.")
    else:
        # Ask user to configure
        log.warning(f"No schemata configured. Use cimpyorm.configure(path_to_schemata) to set-up.")
        from cimpyorm.api import configure

try:
    # See if we already know a datasetroot
    CONFIG["Paths"]["DATASETROOT"] = get_path("DATASETROOT")
    if not os.path.isdir(CONFIG["Paths"]["DATASETROOT"]):
        # Is datasetroot an actual directory?
        log.warning(f"Invalid dataset path in configuration.")
        raise NotADirectoryError
except (KeyError, NotADirectoryError):
    if os.path.isdir(os.path.join(_PACKAGEROOT, "res", "datasets")):
        # Look in the default path
        CONFIG["Paths"]["DATASETROOT"] = os.path.join(_PACKAGEROOT, "res", "datasets")
        log.info(f"Found datasets in default location.")
    else:
        # Ask user to configure
        log.info(f"No datasets configured. Use cimpyorm.configure(path_to_datasets) to set-up.")
        from cimpyorm.api import configure

with open(_CONFIGPATH, "w+") as f:
    # Update config.ini
    CONFIG.write(f)


def describe(element, fmt="psql"):
    element.describe(fmt)


try:
    from cimpyorm.api import parse, load, describe, stats, lint  # pylint: disable=wrong-import-position
    from cimpyorm.Model.Schema import Schema  # pylint: disable=wrong-import-position
except ModuleNotFoundError:
    log.warning(f"Unfulfilled requirements. parse and load are not available.")
