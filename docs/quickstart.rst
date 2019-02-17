====================
Quickstart
====================

********************
Parsing datasets
********************


The parser is invoked using the ``parse`` function:

.. autofunction:: cimpyorm.parse

********************
Loading datasets
********************

Alternatively, an already parsed dataset can be loaded from on-disk files (using SQLite) or from a client-server
database using the ``load`` function:

.. autofunction:: cimpyorm.load

********************
Querying datasets
********************

Queries for CIM objects are performed on the Session objects provided by the :py:func:`~cimpyorm.parse` and
:py:func:`~cimpyorm.load` functions, e.g.::

    db_session, model = parse(r"path_to_dataset")
    acl = db_session.query(model.ACLineSegment).first()

to obtain the first CIM:ACLineSegment from the model.

The objects properties can subsequently be accessed as usual::

    acl.r # Print the ACLineSegment's resistance

The CIM classes' inherited properties (to explore the model's classes and properties see :any:`Exploring`) are also available::

    acl.shortName # shortName is a property of CIM:IdentifiedObjects, which ACLineSegments inherit from

Relationships can be accessed by their name::

    bv = acl.BaseVoltage # Yields the CIM:BaseVoltage object associated with the CIM:ACLineSegment

The model also allows for reverse lookup of relationships that are unidirectional in the CIM standard::

    terminals = acl.Terminals # In the standard, a CIM:ACLineSegment (or rather its base class
                              # CIM:ConductingEquipment) is referenced by a CIM:Terminal definition,
                              # however, cimpyorm adds their inversion for convenience
                              # (the inverse property names are defined in the schema definition)

