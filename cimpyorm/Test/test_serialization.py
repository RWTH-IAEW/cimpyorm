#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

from io import BytesIO
from zipfile import ZipFile

from lxml.etree import _ElementTree, tostring

from cimpyorm import datasets
from cimpyorm.api import serialize, create_empty_dataset, export


def test_serialization_multi():
    s, m = datasets.ENTSOE_FullGrid(refresh=True)
    profiles = {
        "EquipmentProfile": "EQ",
        "DiagramLayoutProfile": "DL",
        "SteadyStateHypothesisProfile": "SSH",
        "TopologyProfile": "TP",
        "GeographicalLocationProfile": "GL",
        "StateVariablesProfile": "SV"}

    trees = serialize(s, mode="Multi", profile_whitelist=profiles.keys())
    assert isinstance(trees, list)
    assert all(isinstance(tree, _ElementTree) for tree in trees)
    with ZipFile(BytesIO(), "w") as zf:
        for profile, tree in zip(profiles.values(), trees):
            zf.writestr(
                f"{profile}.xml",
                tostring(tree,
                         encoding="UTF-8",
                         xml_declaration=True,
                         pretty_print=True)
            )
    s.close()


def test_serialization_attributes():
    s, m = create_empty_dataset(version="16", profile_whitelist=["EQ", "TP", "SSH"])
    terminals = (m.Terminal(id=42, name="somename", phases=m.enum.PhaseCode.v.AB),
                 m.Terminal(id=21, name="someothername", phases=m.enum.PhaseCode.v.ABC))
    s.add_all(terminals)
    _io = export(s, "Single")
    teststr = b'<cim:Terminal rdf:ID="21">\n    <cim:IdentifiedObject.name>someothername</cim:IdentifiedObject.name>\n    <cim:Terminal.phases rdf:resource="http://iec.ch/TC57/2013/CIM-schema-cim16#PhaseCode.ABC"/>\n  </cim:Terminal>'
    assert teststr in _io.getvalue()
    s.close()


def test_manual_profile_header():
    s, m = create_empty_dataset(version="16", profile_whitelist=["EQ", "TP", "SSH"])
    terminals = (m.Terminal(id=42, name="somename", phases=m.enum.PhaseCode.v.AB),
                 m.Terminal(id=21, name="someothername", phases=m.enum.PhaseCode.v.ABC))
    s.add_all(terminals)
    header = {"profile_header":
                  ("http://entsoe.eu/CIM/EquipmentCore/3/1",
                   "http://entsoe.eu/CIM/EquipmentOperation/3/1")}
    tree = serialize(s, mode="Single", header_data=header)
    _str = tostring(tree)
    assert b"http://entsoe.eu/CIM/EquipmentCore/3/1" in _str
    assert b"http://entsoe.eu/CIM/EquipmentOperation/3/1" in _str
    assert b"http://entsoe.eu/CIM/EquipmentShortCircuit/3/1" not in _str
    s.close()


def test_invalid_manual_profile_header():
    s, m = create_empty_dataset(version="16", profile_whitelist=["EQ", "TP", "SSH"])
    terminals = (m.Terminal(id=42, name="somename", phases=m.enum.PhaseCode.v.AB),
                 m.Terminal(id=21, name="someothername", phases=m.enum.PhaseCode.v.ABC))
    s.add_all(terminals)
    header = {"profile_header":
                  ("http://entsoe.eu/CIM/EquipmentCore/3/1",
                   "http://entsoe.eu/CIM/StateVariables/4/1",)}
    tree = serialize(s, mode="Single", header_data=header)
    _str = tostring(tree)
    assert b"http://entsoe.eu/CIM/EquipmentCore/3/1" in _str
    assert b"http://entsoe.eu/CIM/StateVariables/4/1" not in _str
    s.close()


def test_multi_file_manual_profile_header():
    s, m = create_empty_dataset(version="16", profile_whitelist=["EQ", "TP", "SSH"])
    terminals = (m.Terminal(id=42, name="somename", phases=m.enum.PhaseCode.v.AB),
                 m.Terminal(id=21, name="someothername", phases=m.enum.PhaseCode.v.ABC))
    s.add_all(terminals)
    header = {"profile_header":
                  ("http://entsoe.eu/CIM/EquipmentCore/3/1",
                   "http://entsoe.eu/CIM/Topology/4/1",)}
    trees = serialize(s, mode="Multi", header_data=header, profile_whitelist=["EQ", "TP", "SSH"])
    _str = [tostring(tree) for tree in trees]
    assert b"http://entsoe.eu/CIM/EquipmentCore/3/1" in _str[0]
    assert b"http://entsoe.eu/CIM/Topology/4/1" in _str[1]
    assert b"http://entsoe.eu/CIM/StateVariables/4/1" not in _str[0]
    assert b"http://entsoe.eu/CIM/StateVariables/4/1" not in _str[1]
    assert b"http://entsoe.eu/CIM/StateVariables/4/1" not in _str[2]
    s.close()
