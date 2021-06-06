#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

from collections import OrderedDict
from typing import Union
from string import ascii_letters, digits

from sqlalchemy import Column, String, ForeignKey, Boolean, Float, Integer, Table, ForeignKeyConstraint
from sqlalchemy.orm import relationship

from cimpyorm.auxiliary import get_logger, shorten_namespace, XPath
from cimpyorm.Model import auxiliary as aux
from cimpyorm.Model.Elements.Base import ElementMixin, se_ref
from cimpyorm.Model.Elements.Class import CIMClass
from cimpyorm.Model.Elements.Enum import CIMEnumValue, CIMEnum
from cimpyorm.Model.Elements.Datatype import CIMDT

log = get_logger(__name__)


class CIMProp(ElementMixin, aux.Base):
    """
    Class representing a CIM Model property
    """

    # pylint: disable=too-many-instance-attributes

    __tablename__ = "CIMProp"
    XPathMap = None

    prop_name = Column(String(50))

    # Map the property to its class
    cls_name = Column(String(80), primary_key=True)
    cls_namespace = Column(String(30), primary_key=True)
    #: The class this property belongs to.
    cls = relationship(CIMClass, foreign_keys=[cls_name, cls_namespace],
                       backref="props")

    datatype_name = Column(String(50))
    datatype_namespace = Column(String(50))
    #: This property's datatype.
    datatype = relationship(CIMDT,
                            foreign_keys=[datatype_name, datatype_namespace],
                            backref="usedby")

    inverse_property_name = Column(String(80))
    inverse_property_namespace = Column(String(30))
    inverse_class_name = Column(String(80))
    inverse_class_namespace = Column(String(30))
    #: The inverse property associated with this property (None if the property is a Primitive or
    #: an EnumValue)


    inverse = relationship("CIMProp",
                           foreign_keys=[inverse_property_name,
                           inverse_property_namespace, inverse_class_name,
                           inverse_class_namespace], uselist=False)

    used = Column(Boolean)
    multiplicity = Column(String(10))
    many_remote = Column(Boolean)
    optional = Column(Boolean)
    type_ = Column(String(80))

    type = "Generic"

    __table_args__ = (ForeignKeyConstraint((cls_namespace, cls_name),
                                           (CIMClass.namespace_name,
                                            CIMClass.name)),
                      ForeignKeyConstraint((datatype_namespace, datatype_name),
                                           (CIMDT.namespace_name,
                                            CIMDT.name)),
                      ForeignKeyConstraint(("inverse_class_name",
                                            "inverse_class_namespace",
                                            "inverse_property_namespace",
                                            "inverse_property_name"),
                                           ("CIMProp.cls_name",
                                            "CIMProp.cls_namespace", "CIMProp.namespace_name",
                                            "CIMProp.name")),
                      )

    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
        "polymorphic_on": type_
    }

    def __init__(self, schema_elements=None):
        """
        Class constructor
        :param schema_elements: the (merged) xml node element containing the property's description
        """
        super().__init__(schema_elements)
        self._inverseProperty = None
        self.cls_namespace, self.cls_name = self._get_domain()

        _, self.range_name = self._get_range()
        self.range_namespace = self.namespace_name

        self.prop_name = self.name.split(".")[-1]

        self.datatype_namespace, self.datatype_name = self._get_datatype()

        self._set_inverse()

        self.used = self._get_used()
        self.multiplicity = self._multiplicity
        self.many_remote = self._many_remote
        self.optional = self._optional
        self.key = None
        self.var_key = None
        self.xpath = None
        self.association_table = None

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
    def u_key(self):
        r = (se_ref(self.name, self.namespace_name),
             se_ref(self.cls_name, self.cls_namespace))
        return r

    def _set_inverse(self):
        inverse_ns, inverse_name = self._get_inverse()
        if not inverse_name:
            return
        self.inverse_class_name, self.inverse_property_name = inverse_name.split(".")
        self.inverse_property_namespace = self.inverse_class_namespace = inverse_ns

    def _get_used(self):
        """
        Determine whether the property needs to be added to the SQLAlchemy declarative class (i.e. it is not an
        inverseProperty of an existing mapper or it maps to a value, not a reference).

        :return: True if property should be represented in the SQLAlchemy declarative model.
        """
        return bool(self._get_association()) or self.inverse_property_name is None

    # @property
    # def _get_namespace(self) -> Union[str, None]:
    #     return self._extract_namespace(self.name)

    def _get_datatype(self):
        dt = self._get_property("datatype")
        if not dt:
            return None, None
        ns, dt = self._extract_namespace(dt)
        return ns, dt

    @property
    def _multiplicity(self):
        mp = self._get_property("multiplicity")
        return mp.split("M:")[-1] if not isinstance(mp, list) \
            else mp[0].split("M:")[-1]  # pylint: disable=unsubscriptable-object

    def _get_association(self) -> Union[bool, None]:
        association = self._get_property("association")
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

    def _get_inverse(self):
        inverse = self._get_property("inverseRoleName")
        if not inverse:
            return None, None
        ns, inverse = self._extract_namespace(inverse)
        return ns, inverse

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

    def _get_range(self):
        range = self._get_property("range")
        if not range:
            return None, None
        ns, range = self._extract_namespace(range)
        return ns, range

    def _get_domain(self):
        domain = self._get_property("domain")
        ns, domain = self._extract_namespace(domain)
        return ns, domain

    @property
    def mapped_datatype(self):  # pylint: disable=inconsistent-return-statements
        if self.datatype:
            dt = self.datatype
            if dt.base_datatype is not None:
                return dt.base_datatype
            else:
                return dt.name
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

    @property
    def full_name(self):
        return self.namespace.short + "_" + self.name

    def generate(self, nsmap):
        attrs = OrderedDict()
        dt = self.mapped_datatype
        if self.used:
            if isinstance(self.range, CIMEnum):
                var, query_base = self.name_query()
                attrs[f"{var}_name"] = Column(String(120), ForeignKey(CIMEnumValue.name),
                                              name=f"{var}_name")
                attrs[f"{var}_namespace"] = Column(String(120),
                                                   ForeignKey(CIMEnumValue.namespace_name),
                                                   name=f"{var}_namespace")
                attrs[f"{var}_enum_name"] = Column(String(120), ForeignKey(CIMEnumValue.enum_name),
                                                   name=f"{var}_enum_name")
                attrs[f"{var}_enum_namespace"] = Column(String(120),
                                                        ForeignKey(CIMEnumValue.enum_namespace),
                                                        name=f"{var}_enum_namespace")
                attrs[var] = relationship(CIMEnumValue,
                                          foreign_keys=(attrs[f"{var}_name"],
                                                        attrs[f"{var}_namespace"],
                                                        attrs[f"{var}_enum_name"],
                                                        attrs[f"{var}_enum_namespace"]))

                attrs["__table_args__"] = (ForeignKeyConstraint(
                    (attrs[f"{var}_name"], attrs[f"{var}_namespace"], attrs[f"{var}_enum_name"],
                     attrs[f"{var}_enum_namespace"]),
                    (CIMEnumValue.name, CIMEnumValue.namespace_name, CIMEnumValue.enum_name,
                     CIMEnumValue.enum_namespace)
                ),)
                self.key = f"{var}"
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
        self.var_key = self.namespace.short + "_" + self.name if self.namespace.short != "cim" else self.name + end

    def name_query(self):
        try:
            var = self.namespace.short + "_" + self.name if self.namespace.short != "cim" else self.name
        except AttributeError:
            if self.namespace is None:
                raise KeyError(f"Undefined namespace: {self.namespace_name}")
        for _str in (self.namespace.short, self.cls.name, self.name):
            # Make sure there are no funky characters in the XPath query.
            if any((_char not in ascii_letters+digits+"_" for _char in _str)):
                raise ValueError("Malformed XPath-Query.")
        query_base = f"{self.namespace.short}:{self.cls.name}.{self.name}"
        return var, query_base

    def generate_relationship(self, nsmap=None):
        var, query_base = self.name_query()
        attrs = {}
        Map = {}
        log.debug(f"Generating relationship for {var} on {self.name}")
        if self.many_remote:
            if self.inverse:
                br = self.inverse.name if self.namespace.short == "cim" else \
                    self.namespace.short + "_" + self.inverse.name
                tbl = self.generate_association_table()
                self.association_table = tbl
                attrs[var] = relationship(self.range.full_name,
                                          secondary=tbl,
                                          backref=br)
            else:
                tbl = self.generate_association_table()
                attrs[var] = relationship(self.range.full_name,
                                          secondary=tbl)
        else:
            attrs[f"{var}_id"] = Column(String(50),
                                        ForeignKey(f"{self.range.full_name}.id"),
                                        name=f"{var}_id")
            if self.inverse:
                br = self.inverse.name if self.namespace.short == "cim" else \
                    self.namespace.short + "_" + self.inverse.name
                attrs[var] = relationship(self.range.full_name,
                                          foreign_keys=attrs[f"{var}_id"],
                                          backref=br)
            else:
                attrs[var] = relationship(self.range.full_name,
                                          foreign_keys=attrs[f"{var}_id"])
            self.key = f"{var}_id"
        self.xpath = XPath(query_base + "/@rdf:resource", namespaces=nsmap)
        class_ = self.cls.class_
        for attr, attr_value in attrs.items():
            setattr(class_, attr, attr_value)
        return Map

    def generate_association_table(self):
        association_table = Table(f".asn_{self.cls.full_name}_{self.range.full_name}",
                                  aux.Base.metadata,
                                  Column(f"{self.range.full_name}_id", String(50),
                                         ForeignKey(f"{self.range.full_name}.id")),
                                  Column(f"{self.cls.full_name}_id", String(50),
                                         ForeignKey(f"{self.cls.full_name}.id")))
        return association_table


class CIMProp_AlphaNumeric(CIMProp):
    __tablename__ = "CIMProp_AlphaNumeric"
    cls_name = Column(String(80), primary_key=True)
    cls_namespace = Column(String(30), primary_key=True)
    range = None

    type = "Alphanumeric"

    __table_args__ = (ForeignKeyConstraint(("cls_name", "cls_namespace", "namespace_name", "name"),
                                           ("CIMProp.cls_name",
                                            "CIMProp.cls_namespace", "CIMProp.namespace_name",
                                            "CIMProp.name")),
                      ForeignKeyConstraint(("cls_namespace", "cls_name"),
                                           (CIMClass.namespace_name,
                                            CIMClass.name)),
                      )
    __mapper_args__ = {
        "polymorphic_identity": __tablename__
    }

    def __init__(self, schema_elements=None):
        """
        Class constructor
        :param schema_elements: the (merged) xml node element containing the property's description
        """
        super().__init__(schema_elements)


class CIMProp_Reference(CIMProp):
    __tablename__ = "CIMProp_Reference"
    cls_name = Column(String(80), primary_key=True)
    cls_namespace = Column(String(30), primary_key=True)

    range_name = Column(String(80))
    range_namespace = Column(String(30))
    range = relationship(CIMClass, foreign_keys=[range_name, range_namespace],
                         backref="range_elements")

    type = "Reference"

    __table_args__ = (ForeignKeyConstraint(("cls_name","cls_namespace", "namespace_name", "name"),
                                           ("CIMProp.cls_name",
                                            "CIMProp.cls_namespace", "CIMProp.namespace_name",
                                            "CIMProp.name")),
                      ForeignKeyConstraint(("cls_namespace", "cls_name"),
                                           (CIMClass.namespace_name,
                                            CIMClass.name)),
                      ForeignKeyConstraint((range_namespace, range_name),
                                           (CIMClass.namespace_name,
                                            CIMClass.name)),
                      )
    __mapper_args__ = {
        "polymorphic_identity": __tablename__
    }

    def __init__(self, schema_elements=None):
        """
        Class constructor
        :param schema_elements: the (merged) xml node element containing the property's description
        """
        super().__init__(schema_elements)


class CIMProp_Enumeration(CIMProp):
    __tablename__ = "CIMProp_Enumeration"
    cls_name = Column(String(80), primary_key=True)
    cls_namespace = Column(String(30), primary_key=True)

    range_name = Column(String(80))
    range_namespace = Column(String(30))
    range = relationship(CIMEnum, foreign_keys=[range_name, range_namespace],
                         backref="range_elements")

    type = "Enumeration"

    __table_args__ = (ForeignKeyConstraint(("cls_name","cls_namespace", "namespace_name", "name"),
                                           ("CIMProp.cls_name",
                                            "CIMProp.cls_namespace", "CIMProp.namespace_name",
                                            "CIMProp.name")),
                      ForeignKeyConstraint(("cls_namespace", "cls_name"),
                                           (CIMClass.namespace_name,
                                            CIMClass.name)),
                      ForeignKeyConstraint((range_namespace, range_name),
                                           (CIMEnum.namespace_name,
                                            CIMEnum.name)),
                      )
    __mapper_args__ = {
        "polymorphic_identity": __tablename__
    }

    def __init__(self, schema_elements=None):
        """
        Class constructor
        :param schema_elements: the (merged) xml node element containing the property's description
        """
        super().__init__(schema_elements)

    def insert(self, argmap, value):
        argmap[f"{self.key}_name"] = value.split(".")[-1]
        argmap[f"{self.key}_namespace"] = self.namespace.short
        argmap[f"{self.key}_enum_name"] = \
            shorten_namespace(value, self.nsmap).split("_")[-1].split(".")[0]
        argmap[f"{self.key}_enum_namespace"] = self.namespace.short
