from cimpyorm.Model.Elements import CIMClass


def test_persisted_classes(cgmes_schema):
    schema = cgmes_schema
    # Make sure we have all CIMClasses
    assert len(schema.Elements["CIMClass"]) == 397
    assert schema.Elements["CIMClass"]["ACLineSegment"] is \
           schema.session.query(CIMClass).filter(CIMClass.name == "ACLineSegment").one()


def test_summary(cgmes_schema):
    schema = cgmes_schema
    assert schema.model.classes.ACLineSegment.property_table().shape == (27, 8)


def test_description_CIMClass(cgmes_schema):
    from cimpyorm import describe
    describe(cgmes_schema.model.classes.TopologicalNode)
    cgmes_schema.model.classes.TopologicalNode.describe()


def test_description_parseable(cgmes_schema):
    from cimpyorm import describe
    describe(cgmes_schema.model.TopologicalNode)
    cgmes_schema.model.TopologicalNode.describe()
