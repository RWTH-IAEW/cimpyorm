=======================
Exploring
=======================
The schema can be explored using :func:`cimpyorm.describe` resulting in a description of elements being provided.

For the CIM class ACLineSegment::

    from cimpyorm import describe, load
    s, m = load(r"path_to_db")
    describe(m.ACLineSegment)

the following description is printed:

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

==========================  ===================  ==============  ====================  ======  ============  ==========
                     Label               Domain    Multiplicity              Datatype    Unit    Multiplier    Inferred
==========================  ===================  ==============  ====================  ======  ============  ==========
            DiagramObjects     IdentifiedObject            0..n        *DiagramObject       -             -        True
                      mRID     IdentifiedObject            0..1                String       -             -       False
                      name     IdentifiedObject            0..1                String       -             -       False
               description     IdentifiedObject            0..1                String       -             -       False
 entsoe_energyIdentCodeEic     IdentifiedObject            0..1                String       -             -       False
          entsoe_shortName     IdentifiedObject            0..1                String       -             -       False
        energyIdentCodeEic     IdentifiedObject            0..1                String       -             -       False
                 shortName     IdentifiedObject            0..1                String       -             -       False
                  Controls  PowerSystemResource            0..n              *Control       -             -        True
              Measurements  PowerSystemResource            0..n          *Measurement       -             -        True
                  Location  PowerSystemResource            0..1             *Location       -             -        True
        EquipmentContainer            Equipment            0..1   *EquipmentContainer       -             -       False
                 aggregate            Equipment            0..1               Boolean       -             -       False
       OperationalLimitSet            Equipment            0..n  *OperationalLimitSet       -             -        True
                 Terminals  ConductingEquipment            0..n             *Terminal       -             -        True
               BaseVoltage  ConductingEquipment            0..1          *BaseVoltage       -             -       False
                  SvStatus  ConductingEquipment            0..1             *SvStatus       -             -        True
                    length            Conductor            0..1                Length       m             k       False
                       bch        ACLineSegment            1..1           Susceptance       S             -       False
                       gch        ACLineSegment            0..1           Conductance       S             -       False
                         r        ACLineSegment            1..1            Resistance     ohm             -       False
                         x        ACLineSegment            1..1             Reactance     ohm             -       False
                      b0ch        ACLineSegment            1..1           Susceptance       S             -       False
                      g0ch        ACLineSegment            1..1           Conductance       S             -       False
                        r0        ACLineSegment            1..1            Resistance     ohm             -       False
shortCircuitEndTemperature        ACLineSegment            1..1           Temperature    degC             -       False
                        x0        ACLineSegment            1..1             Reactance     ohm             -       False
==========================  ===================  ==============  ====================  ======  ============  ==========