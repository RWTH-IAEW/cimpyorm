from collections import OrderedDict, defaultdict

import pandas as pd
from lxml.etree import XPath
from sqlalchemy import Column, String, ForeignKey, Integer
from sqlalchemy.orm import relationship
from tabulate import tabulate

from cimpyorm.auxiliary import log, shorten_namespace
from cimpyorm.Model.Elements import SchemaElement, CIMPackage, CIMEnum, prefix_ns
from cimpyorm.Model.Parseable import Parseable
from cimpyorm.auxiliary import chunks


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
    @prefix_ns
    def _belongsToCategory(self):
        """
        Return the class' category as determined from the schema
        :return: str
        """
        return self._raw_property("category")

    @property
    @prefix_ns
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

        attrs["_schema_class"] = self

        if self.parent:
            self.class_ = type(self.name, (self.parent.class_,), attrs)
        else: # Base class
            self.class_ = type(self.name, (Parseable, base,), attrs)
        log.debug(f"Defined class {self.name}.")

    def generate(self, nsmap):
        for prop in self.props:
            prop.generate(nsmap)

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
        _all_props = {}
        for prop in self.props:
            if prop.namespace is None or prop.namespace == "cim":
                _all_props[prop.label] = prop
            else:
                _all_props[f"{prop.namespace}_{prop.label}"] = prop
        if self.parent:
            return {**self.parent.all_props, **_all_props}
        else:
            return _all_props

    def parse_values(self, el, session):
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
                        [{f"{prop.domain.label}_id": _id,
                          f"{prop.range.label}_id": _remote_id}
                         for (_id, _remote_id) in chunk])
                    insertables.append(_ins)
            elif len(value) == 1 or len(set(value)) == 1:
                value = value[0]
                if isinstance(prop.range, CIMEnum):
                    argmap[prop.key] = shorten_namespace(value, self.nsmap)
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
            table["Label"].append(key)
            table["Domain"].append(prop.domain.name)
            table["Multiplicity"].append(prop.multiplicity)
            table["Optional"].append(prop.optional)
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
            table["Inferred"].append(not prop.used)
        df = pd.DataFrame(table)
        return df
