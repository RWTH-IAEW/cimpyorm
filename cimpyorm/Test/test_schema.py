from cimpyorm.Model.Elements import CIMClass


def test_persisted_classes(cgmes_schema):
    schema = cgmes_schema
    # Make sure we have all CIMClasses
    assert len(schema.Elements["CIMClass"]) == 397
    assert schema.Elements["CIMClass"]["ACLineSegment"] is \
           schema.session.query(CIMClass).filter(CIMClass.name == "ACLineSegment").one()


def test_summary(cgmes_schema):
    schema = cgmes_schema
    assert schema.model.classes.ACLineSegment.build_summary().shape == (27, 8)
