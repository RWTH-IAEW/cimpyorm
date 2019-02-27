from collections import OrderedDict
from typing import Union

from lxml.etree import XPath
from sqlalchemy import Column, String, ForeignKey, Boolean, Float, Integer, Table
from sqlalchemy.orm import relationship, backref

from cimpyorm.auxiliary import log
from cimpyorm.Model import auxiliary as aux
from cimpyorm.Model.Elements import SchemaElement, CIMPackage, CIMClass, CIMEnumValue, CIMEnum, prefix_ns


class CIMDT(SchemaElement):
    __tablename__ = "CIMDT"
    name = Column(String(80), ForeignKey(SchemaElement.name), primary_key=True)
    package_name = Column(String(50), ForeignKey(CIMPackage.name))
    package = relationship(CIMPackage, foreign_keys=package_name, backref="datatypes")
    stereotype = Column(String(30))

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
        self.package_name = self._category
        self.stereotype = self._stereotype

    @staticmethod
    def _raw_Attributes():
        return {**SchemaElement._raw_Attributes(),
                **{"category": None,
                   "stereotype": None}}

    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {
            "category": XPath(r"cims:belongsToCategory/@rdf:resource", namespaces=cls.nsmap),
            "stereotype": XPath(r"cims:stereotype/text()", namespaces=cls.nsmap)
        }
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    @prefix_ns
    def _stereotype(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return self._raw_property("stereotype")

    @property
    @prefix_ns
    def _category(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return self._raw_property("category")

    @property
    def mapped_datatype(self):
        return self.value.datatype.name


class CIMDTProperty(SchemaElement):
    __tablename__ = "CIMDTProperty"
    name = Column(String(80), ForeignKey(SchemaElement.name), primary_key=True)
    belongs_to_name = Column(String(50), ForeignKey(CIMDT.name))
    belongs_to = relationship(CIMDT, foreign_keys=belongs_to_name, backref="props")
    multiplicity = Column(String(10))
    many_remote = Column(Boolean)

    __mapper_args__ = {
        "polymorphic_identity": __tablename__
    }

    def __init__(self, description):
        """
        Class constructor
        :param description: the (merged) xml node element containing the property's description
        """
        super().__init__(description)
        self.associated_class = None
        self._inverseProperty = None
        self.Attributes = self._raw_Attributes()
        self.belongs_to_name = self._domain
        self.multiplicity = self._multiplicity
        self.many_remote = self._many_remote

    @staticmethod
    def _raw_Attributes():
        return {**SchemaElement._raw_Attributes(),
                **{"namespace": None, "domain": None, "multiplicity": None}}

    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {
            "domain": XPath(r"rdfs:domain/@rdf:resource", namespaces=cls.nsmap),
            "multiplicity": XPath(r"cims:multiplicity/@rdf:resource", namespaces=cls.nsmap)
        }
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    @prefix_ns
    def _domain(self):
        """
        Return the class' category as determined from the schema
        :return: str
        """
        return self._raw_property("domain")

    @property
    @prefix_ns
    def _multiplicity(self):
        mp = self._raw_property("multiplicity")
        return mp.split("M:")[-1] if not isinstance(mp, list) \
            else mp[0].split("M:")[-1]  # pylint: disable=unsubscriptable-object

    @property
    def _many_remote(self):
        if isinstance(self._multiplicity, list):
            return any([mp[-1] in ["2", "n"] for mp in self._multiplicity])  # pylint: disable=not-an-iterable
        else:
            return self._multiplicity[-1] in ["2", "n"]


class CIMDTUnit(CIMDTProperty):
    __tablename__ = "CIMDTUnit"
    name = Column(String(80), ForeignKey(CIMDTProperty.name), primary_key=True)
    belongs_to = relationship(CIMDT, foreign_keys=CIMDTProperty.belongs_to_name, backref=backref("unit", uselist=False))
    symbol_name = Column(String(50), ForeignKey(CIMEnumValue.name))
    symbol = relationship(CIMEnumValue, foreign_keys=symbol_name)

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
        self.symbol_name = self._symbol

    @staticmethod
    def _raw_Attributes():
        return {**CIMDTProperty._raw_Attributes(),
                **{"isFixed": None}}

    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {"isFixed": XPath(r"cims:isFixed/@rdfs:Literal", namespaces=cls.nsmap)}
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    @prefix_ns
    def _symbol(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return f"UnitSymbol.{self._raw_property('isFixed')}"


class CIMDTValue(CIMDTProperty):
    __tablename__ = "CIMDTValue"
    name = Column(String(80), ForeignKey(CIMDTProperty.name), primary_key=True)
    belongs_to = relationship(CIMDT, foreign_keys=CIMDTProperty.belongs_to_name,
                              backref=backref("value", uselist=False))
    datatype_name = Column(String(50), ForeignKey(CIMDT.name))
    datatype = relationship(CIMDT, foreign_keys=datatype_name, backref="values")

    __mapper_args__ = {
        "polymorphic_identity": __tablename__
    }

    def __init__(self, description):
        """
        Class constructor
        :param description: the (merged) xml node element containing the property's description
        """
        super().__init__(description)
        self.Attributes = self._raw_Attributes()
        self.datatype_name = self._datatype

    @staticmethod
    def _raw_Attributes():
        return {**CIMDTProperty._raw_Attributes(), **{"datatype": None}}

    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {"datatype": XPath(r"cims:dataType/@rdf:resource", namespaces=cls.nsmap)}
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    @prefix_ns
    def _datatype(self):
        return self._raw_property("datatype")


class CIMDTMultiplier(CIMDTProperty):
    __tablename__ = "CIMDTMultiplier"
    name = Column(String(80), ForeignKey(CIMDTProperty.name), primary_key=True)
    belongs_to = relationship(CIMDT, foreign_keys=CIMDTProperty.belongs_to_name,
                              backref=backref("multiplier", uselist=False))
    value_name = Column(String(50), ForeignKey(CIMEnumValue.name))
    value = relationship(CIMEnumValue, foreign_keys=value_name)


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
        self.value_name = self._value

    @staticmethod
    def _raw_Attributes():
        return {**CIMDTProperty._raw_Attributes(),
                **{"isFixed": None}}

    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {"isFixed": XPath(r"cims:isFixed/@rdfs:Literal", namespaces=cls.nsmap)}
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    @prefix_ns
    def _value(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return f"UnitMultiplier.{self._raw_property('isFixed')}"


class CIMDTDenominatorUnit(CIMDTProperty):
    __tablename__ = "CIMDTDenominatorUnit"
    name = Column(String(80), ForeignKey(CIMDTProperty.name), primary_key=True)
    belongs_to = relationship(CIMDT, foreign_keys=CIMDTProperty.belongs_to_name,
                              backref=backref("denominator_unit", uselist=False))
    symbol_name = Column(String(50), ForeignKey(CIMEnumValue.name))
    symbol = relationship(CIMEnumValue, foreign_keys=symbol_name)

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
        self.symbol_name = self._symbol

    @staticmethod
    def _raw_Attributes():
        return {**CIMDTProperty._raw_Attributes(),
                **{"isFixed": None}}

    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {"isFixed": XPath(r"cims:isFixed/@rdfs:Literal", namespaces=cls.nsmap)}
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    @prefix_ns
    def _symbol(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return f"UnitSymbol.{self._raw_property('isFixed')}"


class CIMDTDenominatorMultiplier(CIMDTProperty):
    __tablename__ = "CIMDTDenominatorMultiplier"
    name = Column(String(80), ForeignKey(CIMDTProperty.name), primary_key=True)
    belongs_to = relationship(CIMDT, foreign_keys=CIMDTProperty.belongs_to_name,
                              backref=backref("denominator_multiplier", uselist=False))

    value_name = Column(String(50), ForeignKey(CIMEnumValue.name))
    value = relationship(CIMEnumValue, foreign_keys=value_name)

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
        self.value_name = self._value

    @staticmethod
    def _raw_Attributes():
        return {**CIMDTProperty._raw_Attributes(),
                **{"isFixed": None}}

    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {"isFixed": XPath(r"cims:isFixed/@rdfs:Literal", namespaces=cls.nsmap)}
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    @prefix_ns
    def _value(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return f"UnitMultiplier.{self._raw_property('isFixed')}"


class CIMProp(SchemaElement):
    """
    Class representing a CIM Model property
    """

    # pylint: disable=too-many-instance-attributes

    __tablename__ = "CIMProp"
    XPathMap = None

    name = Column(String(80), ForeignKey(SchemaElement.name), primary_key=True)
    prop_name = Column(String(50))
    cls_name = Column(String(50), ForeignKey(CIMClass.name))
    cls = relationship(CIMClass, foreign_keys=cls_name, backref="props")
    datatype_name = Column(String(50), ForeignKey(CIMDT.name))
    datatype = relationship(CIMDT, foreign_keys=datatype_name, backref="usedby")
    inverse_property_name = Column(String(80), ForeignKey("CIMProp.name"))
    inverse = relationship("CIMProp", foreign_keys=inverse_property_name, uselist=False)
    domain_name = Column(String(50), ForeignKey(CIMClass.name))
    domain = relationship(CIMClass, foreign_keys=domain_name, backref="domain_elements")
    range_name = Column(String(50), ForeignKey(CIMClass.name))
    range = relationship(CIMClass, foreign_keys=range_name, backref="range_elements")
    used = Column(Boolean)
    multiplicity = Column(String(10))
    many_remote = Column(Boolean)
    optional = Column(Boolean)

    __mapper_args__ = {
        "polymorphic_identity": __tablename__
    }

    def __init__(self, description):
        """
        Class constructor
        :param description: the (merged) xml node element containing the property's description
        """
        super().__init__(description)
        self._inverseProperty = None
        self.Attributes = self._raw_Attributes()
        self.cls_name = self._domain
        self.prop_name = self.name.split(".")[-1]
        self.datatype_name = self._datatype
        self.inverse_property_name = self._inversePropertyName
        self.domain_name = self._domain
        self.range_name = self._range
        self.used = self._used
        self.multiplicity = self._multiplicity
        self.many_remote = self._many_remote
        self.optional = self._optional
        self.key = None
        self.var_key = None
        self.xpath = None
        self.association_table = None

    @staticmethod
    def _raw_Attributes():
        return {**SchemaElement._raw_Attributes(),
                **{"range": None, "used": None, "association": None, "domain": None, "inverseRoleName": None,
                   "multiplicity": None, "datatype": None, "namespace": None}}


    @classmethod
    def _generateXPathMap(cls):
        super()._generateXPathMap()
        Map = {
            "label": XPath(r"rdfs:label/text()", namespaces=cls.nsmap),
            "association": XPath(r"cims:AssociationUsed/text()", namespaces=cls.nsmap),
            "inverseRoleName": XPath(r"cims:inverseRoleName/@rdf:resource", namespaces=cls.nsmap),
            "datatype": XPath(r"cims:dataType/@rdf:resource", namespaces=cls.nsmap),
            "multiplicity": XPath(r"cims:multiplicity/@rdf:resource", namespaces=cls.nsmap),
            "type": XPath(r"rdf:type/@rdf:resource", namespaces=cls.nsmap),
            "domain": XPath(r"rdfs:domain/@rdf:resource", namespaces=cls.nsmap),
            "range": XPath(r"rdfs:range/@rdf:resource", namespaces=cls.nsmap)
        }
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    def _used(self):
        """
        Determine whether the property needs to be added to the SQLAlchemy declarative class (i.e. it is not an
        inverseProperty of an existing mapper or it maps to a value, not a reference).

        :return: True if property should be represented in the SQLAlchemy declarative model.
        """
        return bool(self._association) or self._inversePropertyName is None

    @property
    @prefix_ns
    def _datatype(self):
        return self._raw_property("datatype")

    @property
    @prefix_ns
    def _multiplicity(self):
        mp = self._raw_property("multiplicity")
        return mp.split("M:")[-1] if not isinstance(mp, list) \
            else mp[0].split("M:")[-1]  # pylint: disable=unsubscriptable-object

    @property
    def _association(self) -> Union[bool, None]:
        association = self._raw_property("association")
        if not association:
            return None
        elif isinstance(association, list):
            if len(set(association)) == 1:
                return association[0] == "Yes" # pylint: disable=E1136
            elif not set(association):
                return None
            else:
                raise ValueError(f"Ambiguous association used parameter for property {self.name}.")
        else:
            return association == "Yes"

    @property
    @prefix_ns
    def _inversePropertyName(self):
        return self._raw_property("inverseRoleName")

    @property
    @prefix_ns
    def _range(self):
        return self._raw_property("range")

    @property
    @prefix_ns
    def _domain(self):
        return self._raw_property("domain")

    @property
    def mapped_datatype(self):  # pylint: disable=inconsistent-return-statements
        if self.datatype:
            if self.datatype.stereotype == "Primitive":
                return self.datatype.name
            elif self.datatype.stereotype == "CIMDatatype":
                return self.datatype.mapped_datatype
        else:
            return None

    @property
    def _many_remote(self):
        if isinstance(self._multiplicity, list):
            return any([mp[-1] in ["2", "n"] for mp in self._multiplicity])  # pylint: disable=not-an-iterable
        else:
            return self._multiplicity[-1] in ["2", "n"]

    @property
    def _optional(self):
        if isinstance(self._multiplicity, list):
            return any([mp.startswith("0") for mp in self._multiplicity])  # pylint: disable=not-an-iterable
        else:
            return self._multiplicity.startswith("0")

    def generate(self, nsmap):
        attrs = OrderedDict()
        dt = self.mapped_datatype
        if self.used:
            if isinstance(self.range, CIMEnum):
                var, query_base = self.name_query()
                attrs[f"{var}_name"] = Column(String(120), ForeignKey(CIMEnumValue.name), name=f"{var}_name")
                attrs[var] = relationship(CIMEnumValue,
                                          foreign_keys=attrs[f"{var}_name"])
                self.key = f"{var}_name"
                self.xpath = XPath(query_base + "/@rdf:resource", namespaces=nsmap)
            elif self.range:
                self.generate_relationship(nsmap)
            elif not self.range:
                var, query_base = self.name_query()
                log.debug(f"Generating property for {var} on {self.name}")
                self.key = var
                self.xpath = XPath(query_base + "/text()", namespaces=nsmap)
                if dt:
                    if dt == "String":
                        attrs[var] = Column(String(50), name=f"{var}")
                    elif dt in ("Float", "Decimal"):
                        attrs[var] = Column(Float, name=f"{var}")
                    elif dt == "Integer":
                        attrs[var] = Column(Integer, name=f"{var}")
                    elif dt == "Boolean":
                        attrs[var] = Column(Boolean, name=f"{var}")
                    else:
                        attrs[var] = Column(String(30), name=f"{var}")
                else:
                    # Fallback to parsing as String(50)
                    attrs[var] = Column(String(50), name=f"{var}")
        for attr, attr_value in attrs.items():
            setattr(self.cls.class_, attr, attr_value)

    def set_var_key(self):
        end = ""
        if isinstance(self.range, CIMEnum):
            end = "_name"
        elif self.range:
            end = "_id"
        self.var_key = self.namespace + "_" + self.label if self.namespace != "cim" else self.label + end

    def name_query(self):
        var = self.namespace + "_" + self.label if self.namespace != "cim" else self.label
        query_base = f"{self.domain.label}.{self.label}" if self.domain.label.startswith(self.namespace) else \
            f"{self.namespace}:{self.domain.label}.{self.label}"
        return var, query_base

    def generate_relationship(self, nsmap=None):
        var, query_base = self.name_query()
        attrs = {}
        Map = {}
        log.debug(f"Generating relationship for {var} on {self.name}")
        if self.many_remote:
            if self.inverse:
                br = self.inverse.label if self.namespace == "cim" else self.namespace + "_" + self.inverse.label
                tbl = self.generate_association_table()
                self.association_table = tbl
                attrs[var] = relationship(self.range.label,
                                          secondary=tbl,
                                          backref=br)
            else:
                tbl = self.generate_association_table()
                attrs[var] = relationship(self.range.label,
                                          secondary=tbl)
        else:
            attrs[f"{var}_id"] = Column(String(50),
                                        ForeignKey(f"{self.range.label}.id"),
                                        name=f"{var}_id")
            if self.inverse:
                br = self.inverse.label if self.namespace == "cim" else self.namespace+"_"+self.inverse.label
                attrs[var] = relationship(self.range.label,
                                          foreign_keys=attrs[f"{var}_id"],
                                          backref=br)
            else:
                attrs[var] = relationship(self.range.label,
                                          foreign_keys=attrs[f"{var}_id"])
            self.key = f"{var}_id"
        self.xpath = XPath(query_base + "/@rdf:resource", namespaces=nsmap)
        class_ = self.cls.class_
        for attr, attr_value in attrs.items():
            setattr(class_, attr, attr_value)
        return Map

    def generate_association_table(self):
        association_table = Table(f".asn_{self.domain.label}_{self.range.label}", aux.Base.metadata,
                                  Column(f"{self.range.label}_id", String(50), ForeignKey(f"{self.range.label}.id")),
                                  Column(f"{self.domain.label}_id", String(50), ForeignKey(f"{self.domain.label}.id")))
        return association_table
