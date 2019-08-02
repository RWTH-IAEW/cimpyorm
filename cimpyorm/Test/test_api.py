import os

import pytest

from cimpyorm import parse, load, lint
from cimpyorm.auxiliary import get_path
from cimpyorm.backends import InMemory


def test_parse_with_schema_directory(full_grid):
    s, m = parse(full_grid, schema=os.path.join(get_path("SCHEMAROOT"), "CIM16"), backend=InMemory)
    assert m.ACLineSegment
    assert s.query(m.Terminal).count() > 0


def test_parse_with_schema_file(full_grid):
    s, m = parse(full_grid, schema=os.path.join(
        get_path("SCHEMAROOT"), "CIM16", "EquipmentProfileCoreRDFSAugmented-v2_4_15-4Jul2016.rdf"), backend=InMemory)
    assert m.ACLineSegment
    assert s.query(m.Terminal).count() > 0
