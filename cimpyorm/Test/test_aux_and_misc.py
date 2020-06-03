#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

from cimpyorm.auxiliary import shorten_namespace


def test_get_class_names_cim(dummy_nsmap):
    assert shorten_namespace(
        frozenset(['{http://iec.ch/TC57/2013/CIM-schema-cim16#}StaticVarCompensator']),
        dummy_nsmap) == ["StaticVarCompensator"]


def test_get_class_names_md(dummy_nsmap):
    assert shorten_namespace(
        frozenset(["{http://iec.ch/TC57/61970-552/ModelDescription/1#}FullModel"]),
        dummy_nsmap) == ["md_FullModel"]


def test_get_class_names_entsoe(dummy_nsmap):
    assert shorten_namespace(
        frozenset(['{http://entsoe.eu/CIM/SchemaExtension/3/1#}EnergySchedulingType']),
        dummy_nsmap) == ["entsoe_EnergySchedulingType"]
