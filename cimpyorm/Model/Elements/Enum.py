from collections import defaultdict

import pandas as pd
from lxml.etree import XPath
from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship
from tabulate import tabulate

from cimpyorm.Model.Elements import SchemaElement, prefix_ns


class CIMEnum(SchemaElement):
    __tablename__ = "CIMEnum"
    name = Column(String(80), ForeignKey(SchemaElement.name), primary_key=True)

    __mapper_args__ = {
        "polymorphic_identity": __tablename__
    }

    def __init__(self, description):
        """
        Class constructor
        :param description: the (merged) xml node element containing the enums's description
        """
        super().__init__(description)
        self.Attributes = self._raw_Attributes()


    @staticmethod
    def _raw_Attributes():
        return {**SchemaElement._raw_Attributes(),
                **{"category": None}}


    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {"category": XPath(r"cims:belongsToCategory/@rdf:resource", namespaces=cls.nsmap)}
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    @prefix_ns
    def _category(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return self._raw_property("category")

    def describe(self, fmt="psql"):
        table = defaultdict(list)
        for value in self.values:
            table["Value"].append(value.label)
        df = pd.DataFrame(table)
        print(tabulate(df, headers="keys", showindex=False, tablefmt=fmt, stralign="right"))


class CIMEnumValue(SchemaElement):
    __tablename__ = "CIMEnumValue"
    name = Column(String(80), ForeignKey(SchemaElement.name), primary_key=True)
    enum_name = Column(String(50), ForeignKey(CIMEnum.name))
    enum = relationship(CIMEnum, foreign_keys=enum_name, backref="values")

    __mapper_args__ = {
        "polymorphic_identity": __tablename__
    }

    def __init__(self, description):
        """
        Class constructor
        :param description: the (merged) xml node element containing the enums's description
        """
        super().__init__(description)
        self.Attributes = self._raw_Attributes()
        self.enum_name = self._enum_name

    @staticmethod
    def _raw_Attributes():
        return {**SchemaElement._raw_Attributes(),
                **{"type": None}}

    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {"type": XPath(r"rdf:type/@rdf:resource", namespaces=cls.nsmap)}
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    @prefix_ns
    def _enum_name(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return self._raw_property("type")
