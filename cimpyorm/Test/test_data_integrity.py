#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

def test_num_of_elements(load_test_db):
    session, m = load_test_db
    assert session.query(m.Terminal).count() == 144


def test_native_properties(load_test_db):
    session, m = load_test_db
    assert isinstance(session.query(m.ACDCConverter.idleLoss).filter(
        m.ACDCConverter.id == "_0f05e270-37ea-471d-89fe-aee8a55b932b"
    ).one()[0], float)
    assert session.query(m.ACDCConverter.idleLoss).filter(
        m.ACDCConverter.id == "_0f05e270-37ea-471d-89fe-aee8a55b932b"
    ).one() == (1.0,)


def test_inherited_properties(load_test_db):
    session, m = load_test_db
    assert session.query(m.Terminal.name).filter(
        m.Terminal.id == "_800ada75-8c8c-4568-aec5-20f799e45f3c"
    ).one() == ("BE-Busbar_2_Busbar_Section",)


def test_relationship(load_test_db):
    session, m = load_test_db
    assert isinstance(session.query(m.Terminal).filter(
        m.Terminal.id == "_800ada75-8c8c-4568-aec5-20f799e45f3c"
    ).one().ConnectivityNode, m.ConnectivityNode)


def test_alter_data(load_test_db):
    session, m = load_test_db
    obj = session.query(m.IdentifiedObject).first()
    obj.entsoe_energyIdentCodeEic = "YetAnotherCode"
    session.commit()
    assert session.query(m.IdentifiedObject).first().entsoe_energyIdentCodeEic == "YetAnotherCode"
