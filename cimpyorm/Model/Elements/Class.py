#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

from collections import OrderedDict, defaultdict

import pandas as pd
from sqlalchemy import Column, String, ForeignKey, Integer, ForeignKeyConstraint
from sqlalchemy.orm import relationship
from tabulate import tabulate

from cimpyorm.Model.Elements.Base import ElementMixin, CIMPackage
from cimpyorm.Model.Parseable import Parseable
from cimpyorm.Model import auxiliary as aux
from cimpyorm.auxiliary import chunks, get_logger, XPath

log = get_logger(__name__)


class CIMClass(ElementMixin, aux.Base):
    """
    A CIM Schema Class (such as Terminal, IdentifiedObject, ...).

    The class definition is read from its XMLS-description.

    :param schema_elements: The XML-Description (an :class:`etree.Element`) defining this CIMClass.

    :param profile: Profile name the element is defined in.
    """
    __tablename__ = "CIMClass"

    package_name = Column(String(80))
    package_namespace = Column(String(30))

    #: The package that contains this class definition.
    package = relationship(CIMPackage,
                           foreign_keys=[package_name, package_namespace],
                           backref="classes")
    parent_name = Column(String(80))
    parent_namespace = Column(String(30))

    #: If this class inherits from a parent class, it is referenced here.
    parent = relationship("CIMClass", foreign_keys=[parent_namespace, parent_name],
                          backref="children", remote_side="CIMClass.name")

    __table_args__ = (ForeignKeyConstraint(("parent_namespace", "parent_name"),
                                           ("CIMClass.namespace_name", "CIMClass.name")),
                      ForeignKeyConstraint((package_namespace, package_name),
                                           (CIMPackage.namespace_name, CIMPackage.name)),
                      )

    def __init__(self, schema_elements=None):
        """
        Class constructor
        :param schema_elements: the (merged) xml node element containing the class's description
        """
        super().__init__(schema_elements)
        self.props = []
        self.class_ = None
        if schema_elements is None:
            return

        self.parent_namespace, self.parent_name = self._get_parent()

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

    def _get_parent(self):
        """
        Returns the parent name and the parent namespace defined in the description.

        :return: (Parent namespace, Parent name)
        """
        parent = self._get_property("parent")
        if parent:
            return self._extract_namespace(parent)[0], parent.lstrip("#")
        else:
            return None, None

    def init_type(self, base):
        """
        Initialize ORM type using the CIMClass object
        :return: None
        """
        log.debug(f"Initializing class {self.full_name}.")
        attrs = OrderedDict()
        attrs["__tablename__"] = self.full_name
        self.Map = dict()
        if self.parent:
            attrs["id"] = Column(String(50), ForeignKey(f"{self.parent.full_name}.id",
                                                        ondelete="CASCADE"), primary_key=True)
            log.debug(f"Created id column on {self.full_name} with FK on {self.parent.full_name}.")
            attrs["__mapper_args__"] = {
                "polymorphic_identity": self.full_name
            }
        else:   # Base class
            attrs["type_"] = Column(String(50))
            attrs["_source_id"] = Column(Integer, ForeignKey("SourceInfo.id"))
            attrs["_source"] = relationship("SourceInfo", foreign_keys=attrs["_source_id"])
            attrs["id"] = Column(String(50), primary_key=True)
            log.debug(f"Created id column on {self.full_name} with no inheritance.")
            attrs["__mapper_args__"] = {
                "polymorphic_on": attrs["type_"],
                "polymorphic_identity": self.full_name}

        attrs["_schema_class"] = self

        if self.parent:
            self.class_ = type(self.full_name, (self.parent.class_,), attrs)
        else: # Base class
            self.class_ = type(self.full_name, (Parseable, base,), attrs)
        log.debug(f"Defined class {self.full_name}.")

    def generate(self, nsmap):
        for prop in self.props:
            prop.generate(nsmap)

    @property
    def prop_keys(self):
        if self.parent:
            return self.parent.prop_keys + [prop.key for prop in self.props]
        else:
            return [prop.key for prop in self.props]

    @property
    def full_name(self):
        return self.namespace.short + "_" + self.name

    @property
    def all_props(self):
        """
        Return all properties (native and inherited) defined for this CIMClass.
        """
        _all_props = {}
        for prop in self.props:
            ns_sensitive_name = prop.name if prop.namespace.short == "cim" \
                else prop.namespace.short + "_" + prop.name
            if ns_sensitive_name in _all_props:
                raise KeyError("Duplicate attribute in hierarchy.")
            _all_props[ns_sensitive_name] = prop
        if self.parent:
            return {**self.parent.all_props, **_all_props}
        else:
            return _all_props

    def parse_values(self, el, session):
        from cimpyorm.Model.Elements.Enum import CIMEnum
        if not self.parent:
            argmap = {}
            insertables = []
        else:
            argmap, insertables = self.parent.parse_values(el, session)
        props = [prop for prop in self.props if prop.used]
        for prop in props:
            value = prop.xpath(el)
            if prop.many_remote and prop.used and value:
                _id = [el.attrib.values()[0]]
                _remote_ids = []
                if len(set(value)) > 1:
                    for raw_value in value:
                        _remote_ids = _remote_ids + [v for v in raw_value.split("#") if len(v)]
                else:
                    _remote_ids = [v for v in value[0].split("#") if len(v)]
                _ids = _id * len(_remote_ids)
                # Insert tuples in chunks of 400 elements max
                for chunk in chunks(list(zip(_ids, _remote_ids)), 400):
                    _ins = prop.association_table.insert(
                        [{f"{prop.cls.full_name}_id": _id,
                          f"{prop.range.full_name}_id": _remote_id}
                         for (_id, _remote_id) in chunk])
                    insertables.append(_ins)
            elif len(value) == 1 or len(set(value)) == 1:
                value = value[0]
                if isinstance(prop.range, CIMEnum):
                    prop.insert(argmap, value)
                else:
                    try:
                        t = prop.mapped_datatype
                        if t == "Float":
                            argmap[prop.key] = float(value)
                        elif t == "Boolean":
                            argmap[prop.key] = value.lower() == "true"
                        elif t == "Integer":
                            argmap[prop.key] = int(value)
                        elif len([v for v in value.split("#") if v]) > 1:
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
        return argmap, insertables

    def to_html(self, **kwargs):
        df = self.property_table()
        return df.to_html(**kwargs)

    def describe(self, fmt="psql"):
        df = self.property_table()
        tab = tabulate(df, headers="keys", showindex=False, tablefmt=fmt, stralign="right")
        c = self
        inh = dict()
        inh["Hierarchy"] = [c.name]
        inh["Number of native properties"] = [len(c.props)]
        while c.parent:
            inh["Hierarchy"].append(c.parent.name)
            inh["Number of native properties"].append(len(c.parent.props))
            c = c.parent
        for val in inh.values():
            val.reverse()
        inh = tabulate(pd.DataFrame(inh),
                       headers="keys", showindex=False, tablefmt=fmt, stralign="right")
        print(inh + "\n" + tab)

    def property_table(self):
        table = defaultdict(list)
        for key, prop in self.all_props.items():
            table["Attribute"].append(key)
            table["Attribute type"].append(prop.type)
            table["Native"].append(prop.used)
            table["Defined in"].append(prop.cls.name)
            table["Optional"].append(prop.optional)
            table["Multiplicity"].append(prop.multiplicity)
            try:
                table["Datatype"].append(prop.datatype.name)
            except AttributeError:
                try:
                    table["Datatype"].append(f"{prop.range.name}")
                except AttributeError:
                    table["Datatype"].append(None)

        df = pd.DataFrame(table)
        return df

    def serialized_properties(self, profile=None):
        from cimpyorm.Model.Elements.Enum import CIMEnum
        namekeys = {}
        for name, prop in self.all_props.items():
            if prop.used:
                if not prop.range:
                    namekeys[prop] = name
                elif isinstance(prop.range, CIMEnum):
                    namekeys[prop] = f"{name}_name"
                elif prop.range:
                    if prop.many_remote:
                        pass # Fixme
                    else:
                        namekeys[prop] = f"{name}_id"
        return namekeys


def highlight_columns(s, cols):
    return ["color: darkblue" if s.name in cols else "color: darkorange" for v in s.index]
