#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

from typing import Union
from collections import namedtuple
from functools import lru_cache

from sqlalchemy import Column, String, ForeignKey, ForeignKeyConstraint, JSON
from sqlalchemy import Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declared_attr

from cimpyorm.auxiliary import get_logger, merge_results, XPath
from cimpyorm.Model import auxiliary as aux

log = get_logger(__name__)
se_type = namedtuple("se_type", ["name", "postpone"], defaults=(False,))
se_ref = namedtuple("se_ref", ["name", "namespace_name"], defaults=("cim",))


mtm_profile_namespace = Table("profile_namespace", aux.Base.metadata,
                              Column("profile_name", String(80), ForeignKey("CIMProfile.name")),
                              Column("namespace_name", String(30), ForeignKey(
                                  "CIMNamespace.short")))

class_used_in = Table("class_profile", aux.Base.metadata,
                      Column("profile_name", String(80), ForeignKey("CIMProfile.name")),
                      Column("class_namespace", String(80)),
                      Column("class_name", String(80)),
                      ForeignKeyConstraint(("class_namespace", "class_name"),
                                           ("CIMClass.namespace_name", "CIMClass.name"))
                      )

prop_used_in = Table("prop_profile", aux.Base.metadata,
                    Column("profile_name", String(80), ForeignKey("CIMProfile.name")),
                    Column("prop_namespace", String(80)),
                    Column("prop_name", String(80)),
                    Column("prop_cls_namespace", String(80)),
                    Column("prop_cls_name", String(80)),
                    ForeignKeyConstraint(("prop_cls_name", "prop_cls_namespace", "prop_namespace", "prop_name"),
                                         ("CIMProp.cls_name", "CIMProp.cls_namespace",
                                          "CIMProp.namespace_name", "CIMProp.name"))
                    )

profile_dep_mandatory = Table("profile_dep_mandatory", aux.Base.metadata,
                              Column("dependant_name", String(80), ForeignKey("CIMProfile.name")),
                              Column("dependency_name", String(80), ForeignKey("CIMProfile.name"))
                              )

profile_dep_optional = Table("profile_dep_optional", aux.Base.metadata,
                             Column("dependant_name", String(80), ForeignKey("CIMProfile.name")),
                             Column("dependency_name", String(80), ForeignKey("CIMProfile.name"))
                             )


class CIMProfile(aux.Base):
    """
    A CIM Profile instance, usually contained in one file.

    This class holds a profile found in a CIM Schema in a SQLAlchemy ORM.

    :param name: The profile's name.
    """
    __tablename__ = "CIMProfile"
    name = Column(String(80), primary_key=True)
    #: The CIM Properties defined in this profile.
    properties = relationship("CIMProp", secondary=prop_used_in, backref="allowed_in")
    #: The CIM Classes used in this profile (not necessarily defined in it).
    classes = relationship("CIMClass", secondary=class_used_in, backref="used_in")
    #: The CIM Classes defined in this profile.
    definitions = {"classes": relationship("CIMClass", backref="defined_in")}
    #: The CIM Datatypes defined in this profile.
    datatypes = relationship("CIMDT")

    mandatory_dependencies = relationship("CIMProfile", secondary=profile_dep_mandatory,
                                          primaryjoin="CIMProfile.name==profile_dep_mandatory.c.dependant_name",
                                          secondaryjoin="CIMProfile.name==profile_dep_mandatory.c.dependency_name")
    optional_dependencies = relationship("CIMProfile", secondary=profile_dep_optional,
                                         primaryjoin="CIMProfile.name==profile_dep_optional.c.dependant_name",
                                         secondaryjoin="CIMProfile.name==profile_dep_optional.c.dependency_name")

    short = Column(String(10))
    uri = Column(JSON)

    #: The CIM namespaces contained in this profile.
    namespaces = relationship(
        "CIMNamespace", secondary=mtm_profile_namespace, back_populates="profiles"
    )

    def __init__(self, name: str, uri: str, short: str):
        self.name: str = name
        self.uri: str = uri
        self.short: str = short


class CIMNamespace(aux.Base):
    """
    A CIM Namespace instance.

    This class holds a namespace found in a CIM Schema in a SQLAlchemy ORM.

    :param short: The namespace's short name.

    :param full_name: The namespace's full URI.
    """
    __tablename__ = "CIMNamespace"
    short = Column(String(30), primary_key=True)
    full_name = Column(String(120))

    #: The CIM profiles that contain this namespace
    profiles = relationship(
        "CIMProfile", secondary=mtm_profile_namespace, back_populates="namespaces"
    )

    def __init__(self, short, full_name):
        self.short = short
        self.full_name = full_name


class ElementMixin:
    """
    Mixin for schema entities.

    This provides common functionality to associate individual SchemaElements (such as classes
    and properties) with namespaces and profiles. The namespace is read from the XMLS-description of
    the element, the profile is provided externally.

    :param schema_elements: The XML-Description (an :class:`etree.Element`) defining this ElementMixin.

    :param profile: Profile name the element is defined in.
    """
    __tablename__ = "ElementMixin"

    @declared_attr
    def defined_in(cls):
        return Column("defined_in", String(80), ForeignKey("CIMProfile.name"))

    @declared_attr.cascading
    def namespace_name(cls):
        return Column("namespace_name", String(30), ForeignKey("CIMNamespace.short"),
                      primary_key=True)

    @declared_attr.cascading
    def name(cls):
        return Column("name", String(80), primary_key=True)

    @declared_attr
    def __table_args__(cls):
        return (ForeignKeyConstraint(("defined_in",), ("CIMProfile.name",)),
                ForeignKeyConstraint(("namespace_name",), ("CIMNamespace.short",)))

    @declared_attr
    def profile(cls):
        return relationship("CIMProfile")

    @declared_attr
    def namespace(cls):
        return relationship("CIMNamespace")

    nsmap = None
    XPathMap = None

    # __columns__ = ("name", "namespace_name")

    def __init__(self, schema_elements=None):
        if schema_elements is None:
            return
        self.schema_elements = schema_elements

        self.name = self._get_name()
        self.namespace_name = self._get_namespace()

        # Fixme: Do we really need the packages? They are sometimes ambiguous. Addintional
        #  many-to-many mappings seem unnecessary
        # self.package_namespace, self.package_name = self._get_package()

        self.Map = None

    @classmethod
    def _generateXPathMap(cls):
        """
        Generator for compiled XPath expressions (those require a namespace_name map to be
        present, hence they are runtime-compiled)
        :return: None
        """
        cls.XPathMap = {"category": XPath(r"cims:belongsToCategory/@rdf:resource",
                                          namespaces=cls.nsmap),
                        "label": XPath(r"rdfs:label/text()", namespaces=cls.nsmap),
                        "stereotype_text": XPath(r"cims:stereotype/text()", namespaces=cls.nsmap)}
        return cls.XPathMap

    def _get_namespace(self) -> Union[str, None]:
        return self._extract_namespace(self.schema_elements.name)[0]

    def _get_name(self) -> Union[str, None]:
        """
        :return: The entities name as defined in its description
        """
        return self._get_property("label")

    def _get_package(self):
        """
        Returns the package name and the packages namespace defined in the description.

        :return: (Package Namespace, Package Name)
        """
        package = self._get_property("category")
        if package:
            return self._extract_namespace(package)[0], package.lstrip("#")
        else:
            return None, None

    @lru_cache()
    def _get_property(self, name) -> Union[list, str, None]:
        """
        Extract a property from the CIM entity
        :param name: property name
        :return: The CIM entity's property as a list, a string, or None
        """
        xp = self.XPathMap
        if name not in xp.keys():
            raise KeyError(f"Invalid name: {name}.")
        results, _ = self.schema_elements.xpath(xp[name])
        try:
            results = merge_results(results)
            return results
        except ValueError:
            log.warning(f"Ambiguous attribute ({name}) for {self.name}.")
            return [result for result in set(results)]

    def describe(self, fmt="psql"):
        print(self)

    @classmethod
    @lru_cache()
    def _extract_namespace(cls, name_attribute):
        ns = None
        for short, full in cls.nsmap.items():
            if name_attribute.startswith(full):
                ns = short
        ns = "cim" if ns is None else ns
        remainder = name_attribute.replace(cls.nsmap[ns], "").lstrip("#")
        return ns, remainder

    @property
    def u_key(self):
        return se_ref(self.name, self.namespace_name)


class CIMPackage(ElementMixin, aux.Base):
    __tablename__ = "CIMPackage"
    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
    }

    def __init__(self, schema_elements=None):
        """
        Class constructor
        :param schema_elements: the (merged) xml node element containing the package's description
        """
        super().__init__(schema_elements)
