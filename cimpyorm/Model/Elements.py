#
#  Copyright (c) 2018 - 2018 Thomas Offergeld (offergeld@ifht.rwth-aachen.de)
#  Institute for High Voltage Technology
#  RWTH Aachen University
#
#  This module is part of cimpyorm.
#
#  cimpyorm is licensed under the BSD-3-Clause license.
#  For further information see LICENSE in the project's root directory.
#

from collections import OrderedDict, defaultdict
from typing import Union

import pandas as pd
from tabulate import tabulate
from lxml import etree
from lxml.etree import XPath
from sqlalchemy import Column, String, ForeignKey, Integer, Float, Boolean
from sqlalchemy.orm import relationship, backref

from cimpyorm import log
import cimpyorm.Model.auxiliary as aux
from cimpyorm.Model.Parseable import Parseable


__all__ = ["CIMPackage", "CIMClass", "CIMProp", "CIMDTProperty",
           "CIMDT", "CIMDTUnit", "CIMDTValue", "CIMDTMultiplier",
           "CIMDTDenominatorUnit", "CIMDTDenominatorMultiplier",
           "CIMEnum", "CIMEnumValue", "SchemaElement"]


class SchemaElement(aux.Base):
    """
    ABC for schema entities.
    """
    __tablename__ = "SchemaElement"
    nsmap = None
    XPathMap = None
    name = Column(String(80), primary_key=True)
    label = Column(String(50))
    namespace = Column(String(30))
    type_ = Column(String(50))
    #comment = Column(String(300))

    __mapper_args__ = {
        "polymorphic_on": type_,
        "polymorphic_identity": __tablename__
    }

    def __init__(self, description=None):
        """
        The ABC's constructor
        :param description: the (merged) xml node element containing the class's description
        """
        if description is None:
            log.error(f"Initialisation of CIM model entity without associated "
                      f"description invalid.")
            raise ValueError(f"Initialisation of CIM model entity without "
                             f"associated description invalid.")
        self.description = description
        self.Attributes = self._raw_Attributes()
        self.name = self._name
        self.label = self._label
        self.namespace = self._namespace
        self.Map = None

    @staticmethod
    def _raw_Attributes():
        return {"name": None, "label": None, "namespace": None}

    @classmethod
    def _generateXPathMap(cls):
        """
        Generator for compiled XPath expressions (those require a namespace map to be present, hence they are compiled
        at runtime)
        :return: None
        """
        cls.XPathMap = {"label": XPath(r"rdfs:label/text()", namespaces=cls.nsmap)}
        return cls.XPathMap

    @property
    @aux.prefix_ns
    def _label(self):
        """
        Return the class' label
        :return: str
        """
        return self._raw_property("label")

    @property
    def _namespace(self) -> Union[str, None]:
        if not self.Attributes["namespace"]:
            if not any(self.name.startswith(ns+"_") for ns in self.nsmap.keys()):
                self.Attributes["namespace"] = "cim"
            else:
                self.Attributes["namespace"] = self.name.split("_")[0]
        return self.Attributes["namespace"]

    @property
    def _comment(self):
        """
        Return the class' label
        :return: str
        """
        # Fixme: This is very slow and not very nice (each string contains the entire xml header - parsing xpath(
        #  "*/text()) doesn't work due to the text containing xml tags). Therefore, this is currently disabled
        str_ = "".join(str(etree.tostring(content, pretty_print=True)) for content in self.description.xpath(
                "rdfs:comment", namespaces=self.nsmap))
        return str_

    @property
    @aux.prefix_ns
    def _name(self) -> Union[str, None]:
        """
        Accessor for an entities name (with cache)
        :return: The entities name as defined in its description
        """
        if self.Attributes["name"]:
            pass
        else:
            _n = self.description.values()[0]
            self.Attributes["name"] = _n
        self.name = self.Attributes["name"]
        return self.Attributes["name"]

    def _raw_property(self, property_identifier) -> Union[list, str, None]:
        """
        Extract a property from the CIM entity
        :param property_identifier: property name
        :return: The CIM entity's property as a list, a string, or None
        """
        if self.Attributes[property_identifier] is None:
            xp = self.XPathMap
            if property_identifier not in xp.keys():
                raise KeyError(f"Invalid property_identifier name {property_identifier}.")
            results = xp[property_identifier](self.description)  # pylint: disable=unsubscriptable-object
            if len(set(results)) == 1:
                self.Attributes[property_identifier] = results[0]
            elif not results:
                self.Attributes[property_identifier] = None
            else:
                log.warning(f"Ambiguous class property_identifier ({property_identifier}) for {self.name}.")
                self.Attributes[property_identifier] = [result for result in set(results)]
        return self.Attributes[property_identifier]


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
    @aux.prefix_ns
    def _category(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return self._raw_property("category")

    def __repr__(self):
        table = defaultdict(list)
        for value in self.values:
            table["Value"].append(value.label)
        df = pd.DataFrame(table)
        return tabulate(df, headers="keys", showindex=False, tablefmt="psql", stralign="right")


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
    @aux.prefix_ns
    def _enum_name(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return self._raw_property("type")


class CIMPackage(SchemaElement):
    __tablename__ = "CIMPackage"
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


class CIMClass(SchemaElement):
    """
    Class representing a CIM Model Class
    """
    __tablename__ = "CIMClass"

    name = Column(String(80), ForeignKey(SchemaElement.name), primary_key=True)
    package_name = Column(String(50), ForeignKey(CIMPackage.name))
    package = relationship(CIMPackage, foreign_keys=package_name, backref="classes")
    parent_name = Column(String(50), ForeignKey("CIMClass.name"))
    parent = relationship("CIMClass", foreign_keys=parent_name, backref="children", remote_side=[name])

    __mapper_args__ = {
        "polymorphic_identity": __tablename__
    }

    def __init__(self, description=None):
        """
        Class constructor
        :param description: the (merged) xml node element containing the class's description
        """
        super().__init__(description)
        self.class_ = None
        self.Attributes = self._raw_Attributes()
        self.package_name = self._belongsToCategory if not \
            isinstance(self._belongsToCategory, list) \
            else self._belongsToCategory[0] # pylint: disable=unsubscriptable-object
        self.parent_name = self._parent_name
        self.props = []

    @staticmethod
    def _raw_Attributes():
        return {**SchemaElement._raw_Attributes(),
                **{"parent": None,
                   "category": None,
                   "namespace": None}
                }

    @classmethod
    def _generateXPathMap(cls):
        """
        Compile XPath Expressions for later use (better performance than tree.xpath(...))
        :return: None
        """
        super()._generateXPathMap()
        Map = {
            "parent": XPath(r"rdfs:subClassOf/@rdf:resource", namespaces=cls.nsmap),
            "category": XPath(r"cims:belongsToCategory/@rdf:resource", namespaces=cls.nsmap)
        }
        if not cls.XPathMap:
            cls.XPathMap = Map
        else:
            cls.XPathMap = {**cls.XPathMap, **Map}

    @property
    @aux.prefix_ns
    def _belongsToCategory(self):
        """
        Return the class' category as determined from the schema
        :return: str
        """
        return self._raw_property("category")

    @property
    @aux.prefix_ns
    def _parent_name(self):
        """
        Return the class' parent as determined from the schema
        :return: str
        """
        return self._raw_property("parent")

    def init_type(self, base):
        """
        Initialize ORM type using the CIMClass object
        :return: None
        """
        log.debug(f"Initializing class {self.name}.")
        attrs = OrderedDict()
        attrs["__tablename__"] = self.name
        self.Map = dict()
        if self.parent:
            attrs["id"] = Column(String(50), ForeignKey(f"{self.parent.name}.id",
                                                        ondelete="CASCADE"), primary_key=True)
            log.debug(f"Created id column on {self.name} with FK on {self.parent.name}.")
            attrs["__mapper_args__"] = {
                "polymorphic_identity": self.name
            }
        else: # Base class
            attrs["type_"] = Column(String(50))
            attrs["_source_id"] = Column(Integer, ForeignKey("SourceInfo.id"))
            attrs["_source"] = relationship("SourceInfo", foreign_keys=attrs["_source_id"])
            attrs["id"] = Column(String(50), primary_key=True)
            log.debug(f"Created id column on {self.name} with no inheritance.")
            attrs["__mapper_args__"] = {
                "polymorphic_on": attrs["type_"],
                "polymorphic_identity": self.name}

        if self.parent:
            self.class_ = type(self.name, (self.parent.class_,), attrs)
        else: # Base class
            self.class_ = type(self.name, (Parseable, base,), attrs)
        log.debug(f"Defined class {self.name}.")

    def generate(self, nsmap):
        for prop in self.props:
            prop.generate(nsmap)

    def parse_data(self, root, attrib):
        """
        Parse objects from snapshot related to this class
        :param root: root of the snapshot XML tree
        :param attrib: "ID" or "about"
        :return: Parsed elements as CIM objects
        """
        nsmap = root.nsmap
        elements = []
        try:
            xml_elements = root.xpath(f"{self.namespace}:{self.label}[@rdf:{attrib}]", namespaces=nsmap)
            if xml_elements:
                for el in xml_elements:
                    # Create Objects of CIM-class
                    if attrib == "ID":
                        # noinspection PyArgumentList
                        elements.append(self.create(el, attrib, nsmap))
                    elif attrib == "about":
                        # noinspection PyArgumentList
                        elements.append(self.create(el, attrib, nsmap))
            else:
                log.debug(f"Skipped {self.namespace}:{self.label}. No elements.")
        except (AttributeError, IndexError) as e:
            raise type(e)(str(e) + f". Class is {self.namespace}:{self.label}")
        if elements:
            log.debug(
                f"Touched {len(elements):,} elements of type "
                f"{self.namespace}:{self.label}.")
        return elements

    def _generate_map(self):
        """
        Generate the parse-map so it finds all properties (even those named after the ancestor in the hierarchy)
        :return: None
        """
        # Make sure the CIM Parent Class is always first in __bases__
        if self.parent:
            self.Map = {**self.parent._generate_map(), **self.Map}  # pylint: disable=no-member
        return self.Map

    @property
    def prop_keys(self):
        if self.parent:
            return self.parent.prop_keys + [prop.key for prop in self.props]
        else:
            return [prop.key for prop in self.props]

    @property
    def all_props(self):
        if self.parent:
            return {**self.parent.all_props, **{prop.label: prop for prop in self.props}}
        else:
            return {prop.label: prop for prop in self.props}

    def create(self, el, attrib, nsmap):  # pylint: disable=inconsistent-return-statements
        if attrib == "ID":
            return self.class_(id=el.get(f"{{{nsmap['rdf']}}}{attrib}"), **self._build_map(el))
        elif attrib == "about":
            return self.class_(_about_ref=el.get(f"{{{nsmap['rdf']}}}{attrib}").replace("#", ""), **self._build_map(el))

    def _build_map(self, el):
        if not self.parent:
            argmap = {}
        else:
            argmap = self.parent._build_map(el)
        props = [prop for prop in self.props if prop.used]
        for prop in props:
            value = prop.xpath(el)
            if len(value) == 1 or len(set(value)) == 1:
                value = value[0]
                if isinstance(prop.range, CIMEnum):
                    argmap[prop.key] = aux.map_enum(value, self.nsmap)
                else:
                    try:
                        t = prop.mapped_datatype
                        if t == "Float":
                            argmap[prop.key] = float(value)
                        elif t == "Boolean":
                            argmap[prop.key] = value.lower() == "true"
                        elif t == "Integer":
                            argmap[prop.key] = int(value)
                        elif len([v for v in value.split("#") if v])>1:
                            log.warning(
                                f"Ambiguous data values for {self.name}:{prop.key}: {len(set(value))} unique values. "
                                f"(Skipped)")
                            # If reference doesn't resolve value is set to None (Validation
                            # has to catch missing obligatory values)
                        else:
                            argmap[prop.key] = value.replace("#", "")
                    except ValueError:
                        argmap[prop.key] = value.replace("#", "")
            elif len(value) > 1:
                log.warning(f"Ambiguous data values for {self.name}:{prop.key}: {len(set(value))} unique values. "
                            f"(Skipped)")
                # If reference doesn't resolve value is set to None (Validation
                # has to catch missing obligatory values)
        return argmap

    def __repr__(self):
        table = defaultdict(list)
        for key, prop in self.all_props.items():
            table["Label"].append(key)
            table["Domain"].append(prop.domain.name)
            table["Multiplicity"].append(prop.multiplicity)
            try:
                table["Datatype"].append(prop.datatype.label)
            except AttributeError:
                table["Datatype"].append(f"*{prop.range.label}")
            try:
                nominator_unit = prop.datatype.unit.symbol.label
                if nominator_unit.lower() == "none":
                    nominator_unit = None
            except AttributeError:
                nominator_unit = None
            try:
                denominator_unit = prop.datatype.denominator_unit.symbol.label
                if denominator_unit.lower() == "none":
                    denominator_unit = None
            except AttributeError:
                denominator_unit = None
            if nominator_unit and denominator_unit:
                table["Unit"].append(f"{nominator_unit}/{denominator_unit}")
            elif nominator_unit:
                table["Unit"].append(f"{nominator_unit}")
            elif denominator_unit:
                table["Unit"].append(f"1/{denominator_unit}")
            else:
                table["Unit"].append("-")

            try:
                nominator_mpl = prop.datatype.multiplier.value.label
                if nominator_mpl.lower() == "none":
                    nominator_mpl = None
            except AttributeError:
                nominator_mpl = None
            try:
                denominator_mpl = prop.datatype.denominator_multiplier.value.label
                if denominator_mpl.lower() == "none":
                    denominator_mpl = None
            except AttributeError:
                denominator_mpl = None
            if nominator_mpl and denominator_mpl:
                table["Multiplier"].append(f"{nominator_mpl}/{denominator_mpl}")
            elif nominator_mpl:
                table["Multiplier"].append(f"{nominator_mpl}")
            elif denominator_mpl:
                table["Multiplier"].append(f"1/{denominator_mpl}")
            else:
                table["Multiplier"].append("-")

        df = pd.DataFrame(table)
        return tabulate(df, headers="keys", showindex=False, tablefmt="psql", stralign="right")


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
    @aux.prefix_ns
    def _stereotype(self):
        """
        Return the enums' category as determined from the schema
        :return: str
        """
        return self._raw_property("stereotype")

    @property
    @aux.prefix_ns
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
    @aux.prefix_ns
    def _domain(self):
        """
        Return the class' category as determined from the schema
        :return: str
        """
        return self._raw_property("domain")

    @property
    @aux.prefix_ns
    def _multiplicity(self):
        mp = self._raw_property("multiplicity")
        return mp.split("M:")[-1] if not isinstance(mp, list) \
            else mp[0].split("M:")[-1]  # pylint: disable=unsubscriptable-object

    @property
    def _many_remote(self):
        if isinstance(self._multiplicity, list):
            return any([mp.endswith("..n") for mp in self._multiplicity])  # pylint: disable=not-an-iterable
        else:
            return self._multiplicity.endswith("..n")


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
    @aux.prefix_ns
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
    @aux.prefix_ns
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
    @aux.prefix_ns
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
    @aux.prefix_ns
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
    @aux.prefix_ns
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
        self.key = None
        self.var_key = None
        self.xpath = None

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
    @aux.prefix_ns
    def _datatype(self):
        return self._raw_property("datatype")

    @property
    @aux.prefix_ns
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
    @aux.prefix_ns
    def _inversePropertyName(self):
        return self._raw_property("inverseRoleName")

    @property
    @aux.prefix_ns
    def _range(self):
        return self._raw_property("range")

    @property
    @aux.prefix_ns
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
            return any([mp.endswith("..n") for mp in self._multiplicity])  # pylint: disable=not-an-iterable
        else:
            return self._multiplicity.endswith("..n")

    def generate(self, nsmap):
        attrs = OrderedDict()
        dt = self.mapped_datatype
        if self.used:
            if isinstance(self.range, CIMEnum):
                var, query_base = self.name_query()
                attrs[f"{var}_name"] = Column(String(50), ForeignKey(CIMEnumValue.name), name=f"{var}_name")
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
