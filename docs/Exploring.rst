=======================
Exploring
=======================
The schema can be explored using :func:`cimpyorm.describe` for a description of elements.

:func:`~cimpyorm.describe` takes a format string to determine table layout (see the
`tabulate documentation <https://bitbucket.org/astanin/python-tabulate>`_).

.. autofunction:: cimpyorm.describe

For the CIM class ACLineSegment::

    from cimpyorm import describe, load
    s, m = load(r"path_to_db")
    describe(m.ACLineSegment)

the following description is printed (the layout and information displayed has changed a bit over
the months):

===================  =============================
          Hierarchy    Number of native properties
===================  =============================
   IdentifiedObject                              8
PowerSystemResource                              3
          Equipment                              3
ConductingEquipment                              3
          Conductor                              1
      ACLineSegment                              9
===================  =============================

==========================  ===================  ==============  ==========  ====================  ======  ============  ==========
                     Label               Domain    Multiplicity    Optional              Datatype    Unit    Multiplier    Inferred
==========================  ===================  ==============  ==========  ====================  ======  ============  ==========
            DiagramObjects     IdentifiedObject            0..n        True        *DiagramObject       -             -        True
                      mRID     IdentifiedObject            0..1        True                String       -             -       False
                      name     IdentifiedObject            1..1       False                String       -             -       False
               description     IdentifiedObject            1..1       False                String       -             -       False
 entsoe_energyIdentCodeEic     IdentifiedObject            0..1        True                String       -             -       False
          entsoe_shortName     IdentifiedObject            1..1       False                String       -             -       False
        energyIdentCodeEic     IdentifiedObject            0..1        True                String       -             -       False
                 shortName     IdentifiedObject            0..1        True                String       -             -       False
                  Controls  PowerSystemResource            0..n        True              *Control       -             -        True
              Measurements  PowerSystemResource            0..n        True          *Measurement       -             -        True
                  Location  PowerSystemResource            0..1        True             *Location       -             -        True
        EquipmentContainer            Equipment            0..1        True   *EquipmentContainer       -             -       False
                 aggregate            Equipment            0..1        True               Boolean       -             -       False
       OperationalLimitSet            Equipment            0..n        True  *OperationalLimitSet       -             -        True
                 Terminals  ConductingEquipment            0..n        True             *Terminal       -             -        True
               BaseVoltage  ConductingEquipment            0..1        True          *BaseVoltage       -             -       False
                  SvStatus  ConductingEquipment            0..1        True             *SvStatus       -             -        True
                    length            Conductor            0..1        True                Length       m             k       False
                       bch        ACLineSegment            1..1       False           Susceptance       S             -       False
                       gch        ACLineSegment            0..1        True           Conductance       S             -       False
                         r        ACLineSegment            1..1       False            Resistance     ohm             -       False
                         x        ACLineSegment            1..1       False             Reactance     ohm             -       False
                      b0ch        ACLineSegment            1..1       False           Susceptance       S             -       False
                      g0ch        ACLineSegment            1..1       False           Conductance       S             -       False
                        r0        ACLineSegment            1..1       False            Resistance     ohm             -       False
shortCircuitEndTemperature        ACLineSegment            1..1       False           Temperature    degC             -       False
                        x0        ACLineSegment            1..1       False             Reactance     ohm             -       False
==========================  ===================  ==============  ==========  ====================  ======  ============  ==========

**************************
CIM-Explorer
**************************
In addition the schema model for the CGMES v2.4.15 is provided `by a related project
<https://cimflaskexplorer.herokuapp.com/>`_.