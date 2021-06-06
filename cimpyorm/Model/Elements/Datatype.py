#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

from sqlalchemy import Column, String, ForeignKeyConstraint
from sqlalchemy.orm import relationship

from cimpyorm.Model import auxiliary as aux
from cimpyorm.auxiliary import apply_xpath, merge_results
from cimpyorm.Model.Elements.Base import ElementMixin, CIMPackage
from cimpyorm.auxiliary import XPath


class CIMDT(ElementMixin, aux.Base):
    """
    A CIM Datatype object, uniquely identified by its name, profile_name and namespace_name.

    Belongs to a CIMPackage and has a Datatype-Unit and a Datatype-Multiplier for its nominator
    and denominator.
    """
    __tablename__ = "CIMDT"
    package_name = Column(String(80))
    package_namespace = Column(String(30))
    package = relationship(CIMPackage,
                           foreign_keys=[package_namespace, package_name],
                           backref="datatypes")

    nominator_unit = Column(String(30))
    denominator_unit = Column(String(30))

    nominator_multiplier = Column(String(30))
    denominator_multiplier = Column(String(30))

    base_datatype = Column(String(80))
    
    stereotype = Column(String(30))
    __table_args__ = (ForeignKeyConstraint((package_namespace, package_name),
                                           (CIMPackage.namespace_name,
                                            CIMPackage.name)),
                      ForeignKeyConstraint(("defined_in",), ("CIMProfile.name",)),
                      ForeignKeyConstraint(("namespace_name",), ("CIMNamespace.short",))
                      )

    def __init__(self, schema_elements=None):
        """
        Class constructor
        :param schema_elements: the (merged) xml node element containing the enums's description
        """
        super().__init__(schema_elements)
        self.stereotype = self._stereotype

    def __str__(self):
        if self.base_datatype:
            return f"CIM Datatype: {self.name} with base datatype {self.base_datatype}."
        else:
            return f"CIM base Datatype: {self.name}."

    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {
            "stereotype": XPath(r"cims:stereotype/text()", namespaces=cls.nsmap),
            "datatype": XPath(r"cims:dataType/@rdf:resource", namespaces=cls.nsmap),
            "isFixed": XPath(r"cims:isFixed/@rdfs:Literal", namespaces=cls.nsmap)
        }
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    def _stereotype(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return self._get_property("stereotype")

    @property
    def mapped_datatype(self):
        return self.name

    def set_datatype(self, elements):
        val, _ = apply_xpath(self.XPathMap["datatype"], elements)
        val = merge_results(val)
        val = val.lstrip("#")
        self.base_datatype = val

    def set_multiplier(self, elements, type):
        val, _ = apply_xpath(self.XPathMap["isFixed"], elements)
        val = merge_results(val)
        if type == "nominator":
            self.nominator_multiplier = val
        elif type == "denominator":
            self.denominator_multiplier = val

    def set_unit(self, elements, type):
        val, _ = apply_xpath(self.XPathMap["isFixed"], elements)
        val = merge_results(val)
        if type == "nominator":
            self.nominator_unit = val
        elif type == "denominator":
            self.denominator_unit = val
