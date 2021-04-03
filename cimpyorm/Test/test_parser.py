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
from importlib.resources import path
import os
import inspect

from defusedxml.lxml import fromstring

from cimpyorm import parse, backends
from cimpyorm.Test import test_datasets

p_test_datasets = os.path.dirname(inspect.getfile(test_datasets))


def test_single_object(cgmes_schema):
    ACL = cgmes_schema.model.classes.ACLineSegment
    literal = '<?xml version="1.0" encoding="UTF-8"?>' \
        '<rdf:RDF  xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#" xmlns:entsoe="http://entsoe.eu/CIM/SchemaExtension/3/1#" xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">' \
        '	<cim:ACLineSegment rdf:ID="_17086487-56ba-4979-b8de-064025a6b4da">' \
        '		<cim:IdentifiedObject.name>BE-Line_1</cim:IdentifiedObject.name>' \
        '		<cim:Equipment.EquipmentContainer rdf:resource="#_2b659afe-2ac3-425c-9418-3383e09b4b39"/>' \
        '		<cim:ACLineSegment.r>2.200000</cim:ACLineSegment.r>' \
        '		<cim:ACLineSegment.x>68.200000</cim:ACLineSegment.x>' \
        '		<cim:ACLineSegment.bch>0.0000829380</cim:ACLineSegment.bch>' \
        '		<cim:Conductor.length>22.000000</cim:Conductor.length>' \
        '		<cim:ACLineSegment.gch>0.0000308000</cim:ACLineSegment.gch>' \
        '		<cim:Equipment.aggregate>false</cim:Equipment.aggregate>' \
        '		<cim:ConductingEquipment.BaseVoltage rdf:resource="#_7891a026ba2c42098556665efd13ba94"/>' \
        '		<cim:ACLineSegment.r0>6.600000</cim:ACLineSegment.r0>' \
        '		<cim:ACLineSegment.x0>204.600000</cim:ACLineSegment.x0>' \
        '		<cim:ACLineSegment.b0ch>0.0000262637</cim:ACLineSegment.b0ch>' \
        '		<cim:ACLineSegment.g0ch>0.0000308000</cim:ACLineSegment.g0ch>' \
        '		<cim:ACLineSegment.shortCircuitEndTemperature>160.0000000000</cim:ACLineSegment.shortCircuitEndTemperature>' \
        '		<entsoe:IdentifiedObject.shortName>BE-L_1</entsoe:IdentifiedObject.shortName>' \
        '		<entsoe:IdentifiedObject.energyIdentCodeEic>10T-AT-DE-000061</entsoe:IdentifiedObject.energyIdentCodeEic>' \
        '		<cim:IdentifiedObject.description>10T-AT-DE-000061</cim:IdentifiedObject.description>' \
        '		<cim:IdentifiedObject.mRID>17086487-56ba-4979-b8de-064025a6b4da</cim:IdentifiedObject.mRID>' \
        '	</cim:ACLineSegment>' \
        '</rdf:RDF>'
    map = {'mRID': '17086487-56ba-4979-b8de-064025a6b4da',
            'name': 'BE-Line_1',
            'description': '10T-AT-DE-000061',
            'entsoe_energyIdentCodeEic': '10T-AT-DE-000061',
            'entsoe_shortName': 'BE-L_1',
            'EquipmentContainer_id': '_2b659afe-2ac3-425c-9418-3383e09b4b39',
            'aggregate': False,
            'BaseVoltage_id': '_7891a026ba2c42098556665efd13ba94',
            'length': 22.0,
            'bch': 8.2938e-05,
            'gch': 3.08e-05,
            'r': 2.2,
            'x': 68.2,
            'b0ch': 2.62637e-05,
            'g0ch': 3.08e-05,
            'r0': 6.6,
            'shortCircuitEndTemperature': 160.0,
            'x0': 204.6}
    assert ACL.parse_values(fromstring(literal.encode("UTF-8"))[0], cgmes_schema.session)[0] == map


def assert_complete_basic_terminal_element(s, m):
    t = s.query(m.Terminal).one()
    assert t.phases == m.enum.PhaseCode.v.ABC
    assert t.sequenceNumber == 1
    assert t.ConductingEquipment_id == "_1e7f52a9-21d0-4ebe-9a8a-b29281d5bfc9" # The object
    # doesn't exist, so t.ConductingEquipment is None
    assert t.name == "L5_0"

    # The other attributes are added by extension profiles (TP and SSH), so if these fail
    # something is wrong with the object-merge
    assert t.TopologicalNode_id == "_37edd845-456f-4c3e-98d5-19af0c1cef1e"
    assert t.connected == True
    s.close()


def test_merge_across_profiles():
    dataset = os.path.join(p_test_datasets, "basic_mergeable_dataset")
    s, m = parse(dataset, backend=backends.InMemory)
    assert_complete_basic_terminal_element(s, m)


def test_merge_w_inconsistent_classnames():
    dataset = os.path.join(p_test_datasets, "mergeable_dataset_w_inconsistent_class_definitions")
    s, m = parse(dataset, backend=backends.InMemory)
    assert_complete_basic_terminal_element(s, m)


def test_reverse_order_uuid_usage():
    # In this testcase the merge order of the id- and about-object references is changed. This
    # should not be a problem
    dataset = os.path.join(p_test_datasets, "basic_reverse_order_merge")
    s, m = parse(dataset, backend=backends.InMemory)
    assert_complete_basic_terminal_element(s, m)


def test_too_generic_object_declaration():
    # In this testcase the merge order of the id- and about-object references is changed. This
    # should not be a problem
    with pytest.raises(ValueError):
        dataset = os.path.join(p_test_datasets, "terminal_declaration_too_generic")
        s, m = parse(dataset, backend=backends.InMemory)


one_node = \
    '<rdf:RDF  xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#" xmlns:entsoe="http://entsoe.eu/CIM/SchemaExtension/3/1#" xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'\
    '<cim:TopologicalIsland rdf:ID="_7f28263d-4f21-c942-be2e-3c6b8d54c546">'\
    '<cim:IdentifiedObject.name>TOP_NET_1</cim:IdentifiedObject.name>'\
    '<cim:TopologicalIsland.AngleRefTopologicalNode rdf:resource="#_a81d08ed-f51d-4538-8d1e-fb2d0dbd128e"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="'\
    '#_f6ee76f7-3d28-6740-aa78-f0bf7176cdad'\
    '#_514fa0d5-a432-5743-8204-1c8518ffed76'\
    '#_ac279ca9-c4e2-0145-9f39-c7160fff094d'\
    '#_f70f6bad-eb8d-4b8f-8431-4ab93581514e'\
    '#_a81d08ed-f51d-4538-8d1e-fb2d0dbd128e'\
    '#_f96d552a-618d-4d0c-a39a-2dea3c411dee'\
    '#_5c74cb26-ce2f-40c6-951d-89091eb781b6'\
    '#_4c66b132-0977-1e4c-b9bb-d8ce2e912e35'\
    '#_52dc7463-7646-b244-8b12-eb57fbd30eab'\
    '#_c21be5da-d2a6-d94f-8dcb-92e4d6fa48a7'\
    '#_d3d9c515-2ddb-436a-bf17-2f8be2394de3'\
    '#_902e51fc-8487-4d9d-ba3a-7dcfcfeef4d1'\
    '#_3aad8a0b-d1d4-4ee2-9690-4c7106be4530'\
    '#_e44141af-f1dc-44d3-bfa4-b674e5c953d7'\
    '#_99b219f3-4593-428b-a4da-124a54630178'\
    '#_27d57afa-6c9d-4b06-93ea-8c88d14af8b1'\
    '#_ac772dd8-7910-443f-8af0-a7fca0fb57f9'\
    '#_b01fe92f-68ab-4123-ae45-f22d3e8daad1'\
    '#_9f1860f9-2110-4a36-b0a0-f75126040d29'\
    '#_c142012a-b652-4c03-9c35-aa0833e71831"/>'\
    '<cim:IdentifiedObject.mRID>7f28263d-4f21-c942-be2e-3c6b8d54c546</cim:IdentifiedObject.mRID>'\
    '</cim:TopologicalIsland>'\
    '</rdf:RDF>'
multi_node = \
    '<rdf:RDF  xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#" xmlns:entsoe="http://entsoe.eu/CIM/SchemaExtension/3/1#" xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'\
    '<cim:TopologicalIsland rdf:ID="_7f28263d-4f21-c942-be2e-3c6b8d54c546">'\
    '<cim:IdentifiedObject.name>TOP_NET_1</cim:IdentifiedObject.name>'\
    '<cim:TopologicalIsland.AngleRefTopologicalNode rdf:resource="#_a81d08ed-f51d-4538-8d1e-fb2d0dbd128e"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_f6ee76f7-3d28-6740-aa78-f0bf7176cdad"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_514fa0d5-a432-5743-8204-1c8518ffed76"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_ac279ca9-c4e2-0145-9f39-c7160fff094d"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_f70f6bad-eb8d-4b8f-8431-4ab93581514e"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_a81d08ed-f51d-4538-8d1e-fb2d0dbd128e"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_f96d552a-618d-4d0c-a39a-2dea3c411dee"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_5c74cb26-ce2f-40c6-951d-89091eb781b6"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_4c66b132-0977-1e4c-b9bb-d8ce2e912e35"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_52dc7463-7646-b244-8b12-eb57fbd30eab"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_c21be5da-d2a6-d94f-8dcb-92e4d6fa48a7"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_d3d9c515-2ddb-436a-bf17-2f8be2394de3"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_902e51fc-8487-4d9d-ba3a-7dcfcfeef4d1"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_3aad8a0b-d1d4-4ee2-9690-4c7106be4530"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_e44141af-f1dc-44d3-bfa4-b674e5c953d7"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_99b219f3-4593-428b-a4da-124a54630178"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_27d57afa-6c9d-4b06-93ea-8c88d14af8b1"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_ac772dd8-7910-443f-8af0-a7fca0fb57f9"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_b01fe92f-68ab-4123-ae45-f22d3e8daad1"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_9f1860f9-2110-4a36-b0a0-f75126040d29"/>'\
    '<cim:TopologicalIsland.TopologicalNodes rdf:resource="#_c142012a-b652-4c03-9c35-aa0833e71831"/>'\
    '<cim:IdentifiedObject.mRID>7f28263d-4f21-c942-be2e-3c6b8d54c546</cim:IdentifiedObject.mRID>'\
    '</cim:TopologicalIsland>'\
    '</rdf:RDF>'


@pytest.mark.parametrize("literal", [one_node, multi_node], ids=["single_property_node", "multiple_property_nodes"])
def test_m2m_rel(cgmes_schema, literal):
    TI = cgmes_schema.model.classes.TopologicalIsland
    insertable = TI.parse_values(fromstring(literal.encode("UTF-8"))[0], cgmes_schema.session)[1][0]
    values = insertable.parameters
    assert "cim_TopologicalIsland_id" in values[0].keys()
    assert "cim_TopologicalNode_id" in values[0].keys()
    assert "_f6ee76f7-3d28-6740-aa78-f0bf7176cdad" in [value["cim_TopologicalNode_id"] for value in values]
    assert len(values) == 20
