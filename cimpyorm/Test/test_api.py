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

from cimpyorm.api import create_empty_dataset
from cimpyorm import parse, load, lint
from cimpyorm.auxiliary import get_path
from cimpyorm.backends import InMemory


def test_parse_with_schema_directory(full_grid):
    s, m = parse(full_grid, schema=os.path.join(get_path("SCHEMAROOT"), "CIM16"), backend=InMemory)
    assert m.ACLineSegment
    assert s.query(m.Terminal).count() > 0


def test_empty_dataset():
    s, m = create_empty_dataset(version="16")
    term = m.Terminal(id=42)
    s.add(term)
    s.commit()


def test_empty_dataset_profile_whitelist():
    s, m = create_empty_dataset(version="16", profile_whitelist=
    ("EquipmentProfile", "TopologyProfile", "SteadyStateHypothesisProfile"))
    term = m.Terminal(id=42)
    s.add(term)
    s.commit()


def test_empty_dataset_profile_whitelist_all_allowed():
    s, m = create_empty_dataset(version="16", profile_whitelist=
    ["EquipmentProfile", "TopologyProfile", "SteadyStateHypothesisProfile",
     "DiagramLayoutProfile", "StateVariablesProfile", "SteadyStateHypothesisProfile",
     "GeographicalLocationProfile", "EquipmentBoundaryProfile", "TopologyBoundaryProfile"])
    term = m.Terminal(id=42)
    s.add(term)
    s.commit()


def test_empty_dataset_profile_whitelist_fullnames():
    s, m = create_empty_dataset(version="16", profile_whitelist=
    ["EquipmentProfile", "EquipmentBoundaryProfile"])
    term = m.Terminal(id=42)
    s.add(term)
    s.commit()


def test_empty_dataset_profile_whitelist_shortnames():
    s, m = create_empty_dataset(version="16", profile_whitelist=["EQ", "EQ_BD"])
    term = m.Terminal(id=42)
    s.add(term)
    s.commit()


def test_empty_dataset_one_profile():
    with pytest.raises(KeyError):
        s, m = create_empty_dataset(version="16", profile_whitelist=["EQ",])
    # This is expected to fail since the CGMES 2.4.15 Schema definitions contain references to the entsoe-Namespace
    # in the EquipmentProfile, but do not define the entsoe-Namespace in the header.


def test_empty_dataset_profile_whitelist_invalid_object():
    _, partial = create_empty_dataset(version="16", backend=InMemory,
                                      profile_whitelist=("EquipmentProfile", "TopologyProfile",
                                                         "SteadyStateHypothesisProfile"))
    _, full = create_empty_dataset(version="16", backend=InMemory)
    with pytest.raises(AttributeError):
        partial.DiagramObjectPoint()
    full.DiagramObjectPoint()
