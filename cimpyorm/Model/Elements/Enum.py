#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

from collections import defaultdict
from typing import Union

import pandas as pd
from sqlalchemy import Column, String, ForeignKeyConstraint
from sqlalchemy.orm import relationship
from tabulate import tabulate

from cimpyorm.Model.Elements.Base import ElementMixin, se_ref
from cimpyorm.Model import auxiliary as aux
from cimpyorm.auxiliary import XPath


class CIMEnum(ElementMixin, aux.Base):
    __tablename__ = "CIMEnum"

    def __init__(self, schema_elements=None):
        """
        Class constructor
        :param schema_elements: the (merged) xml node element containing the enums's description
        """
        super().__init__(schema_elements)

    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {"category": XPath(r"cims:belongsToCategory/@rdf:resource", namespaces=cls.nsmap)}
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    def _get_namespace(self) -> Union[str, None]:
        stereotyped_namespace = self._get_property("stereotype_text")
        if stereotyped_namespace and stereotyped_namespace == "Entsoe":
            # Fixme: This is hardcoded as the "Entsoe" stereotype determines the namespace for
            #  some properties. However, the same attribute is sometimes used to denote the CIM
            #  Package (e.g. ShortCircuit) which should not be misinterpreted as a namespace.
            return "entsoe"
        else:
            # Determine from name
            return self._extract_namespace(self.schema_elements.name)[0]

    def describe(self, fmt="psql"):
        table = defaultdict(list)
        for value in self.values:
            table["Value"].append(value.label)
        df = pd.DataFrame(table)
        print(tabulate(df, headers="keys", showindex=False, tablefmt=fmt, stralign="right"))

    def to_html(self, **kwargs):
        df = self.property_table()
        return df.to_html(**kwargs)

    def property_table(self):
        return pd.DataFrame({"Values": [value.name for value in self.values]})


class CIMEnumValue(ElementMixin, aux.Base):
    __tablename__ = "CIMEnumValue"
    enum_name = Column(String(80), primary_key=True)
    enum_namespace = Column(String(30), primary_key=True)
    enum = relationship(CIMEnum, foreign_keys=enum_name, backref="values")

    fqn = Column(String(120))
    __table_args__ = (ForeignKeyConstraint((enum_namespace,
                                            enum_name),
                                           (CIMEnum.namespace_name,
                                            CIMEnum.name)),)

    def __init__(self, schema_elements=None):
        """
        Class constructor
        :param schema_elements: the (merged) xml node element containing the enums's description
        """
        super().__init__(schema_elements)
        self.enum_namespace, self.enum_name = self._get_enum()

    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {"type": XPath(r"rdf:type/@rdf:resource", namespaces=cls.nsmap)}
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    def _get_enum(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        domain = self._get_property("type")
        ns, domain = self._extract_namespace(domain)
        return ns, domain

    @property
    def u_key(self):
        r = (se_ref(self.name, self.namespace_name),
             se_ref(self.enum_name, self.enum_namespace))
        return r
