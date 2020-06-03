#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

import pytest
import os

# Keep import for _CONFIGPATH - otherwise get_path fails because cimpyorm/__init__.py locals aren't present

from cimpyorm.auxiliary import log, get_path


@pytest.fixture(scope="session")
def full_grid():
    try:
        path = os.path.join(get_path("DATASETROOT"), "FullGrid")
    except KeyError:
        pytest.skip(f"Dataset path not configured")
    if not os.path.isdir(path) or not os.listdir(path):
        pytest.skip("Dataset 'FullGrid' not present.")
    else:
        return path


@pytest.fixture(scope="module")
def acquire_db():
    import cimpyorm.backends
    backend = cimpyorm.backends.SQLite()
    engine = backend.engine
    session = backend.ORM
    return engine, session


@pytest.fixture(scope="session")
def load_test_db():
    """
    Returns a session and a model for a database that's only supposed to be read from
    :return: session, m
    """
    from cimpyorm.api import load
    path = os.path.join(get_path("DATASETROOT"), "FullGrid", "StaticTest.db")
    if not os.path.isfile(path):
        pytest.skip("StaticTest.db not present.")
    session, m = load(path)
    return session, m


@pytest.fixture(scope="session")
def dummy_source():
    try:
        path = os.path.join(get_path("DATASETROOT"), "FullGrid", "20171002T0930Z_BE_EQ_4.xml")
    except KeyError:
        pytest.skip(f"Dataset path not configured")
    if not os.path.isfile(path):
        pytest.skip("Dataset 'FullGrid' not present.")
    from cimpyorm.Model.Source import SourceInfo
    ds = SourceInfo(source_file=path)
    return ds


@pytest.fixture(scope="session")
def dummy_nsmap():
    from cimpyorm.auxiliary import HDict
    nsmap = HDict({'cim': 'http://iec.ch/TC57/2013/CIM-schema-cim16#',
                   'entsoe': 'http://entsoe.eu/CIM/SchemaExtension/3/1#',
                   'md': 'http://iec.ch/TC57/61970-552/ModelDescription/1#',
                   'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'})
    return nsmap


@pytest.fixture(scope="session")
def cgmes_schema():
    from cimpyorm.Model.Schema import Schema
    schema = Schema(version="16")
    return schema
