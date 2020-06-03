#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

from cimpyorm.Model.Elements import CIMClass


def test_persisted_classes(cgmes_schema):
    schema = cgmes_schema
    # Make sure we have all CIMClasses
    assert len(schema.Elements["CIMClass"]) == 397
    # Make sure the schema classes can be rebuilt from the database
    assert schema.model.ACLineSegment is \
           schema.session.query(CIMClass).filter(CIMClass.name == "ACLineSegment").one().class_


def test_summary(cgmes_schema):
    schema = cgmes_schema
    assert schema.model.classes.ACLineSegment.property_table().shape == (25, 7)


def test_description_CIMClass(cgmes_schema):
    from cimpyorm import describe
    describe(cgmes_schema.model.classes.TopologicalNode)
    cgmes_schema.model.classes.TopologicalNode.describe()


def test_description_CIMClasswType(cgmes_schema):
    from cimpyorm import describe
    describe(cgmes_schema.model.classes.OperationalLimitType)
    cgmes_schema.model.classes.OperationalLimitType.describe()


def test_description_parseable(cgmes_schema):
    from cimpyorm import describe
    describe(cgmes_schema.model.TopologicalNode)
    cgmes_schema.model.TopologicalNode.describe()


def test_selective_profiles():
    from cimpyorm.Model.Schema import Schema
    schema = Schema(version="16", profile_whitelist=
    ("EquipmentProfile", "TopologyProfile", "SteadyStateHypothesisProfile"))
    assert len(schema.model.classes.__dict__) == 169
