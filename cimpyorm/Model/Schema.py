#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

import json
from argparse import Namespace
import os
from collections import ChainMap, Iterable, defaultdict

from defusedxml.lxml import parse
import networkx as nx
from networkx import DiGraph, bfs_tree, dfs_tree
from networkx.exception import NetworkXNoPath
from sqlalchemy import TEXT, Integer, Column
from sqlalchemy.exc import InvalidRequestError, OperationalError

from cimpyorm.auxiliary import HDict, merge_descriptions, find_rdfs_path, get_logger, apply_xpath, XPath
from cimpyorm.Model.Elements.Base import CIMNamespace, CIMProfile, prop_used_in, se_type, CIMPackage, ElementMixin, \
    se_ref

from cimpyorm.Model.Elements.Enum import CIMEnum, CIMEnumValue
from cimpyorm.Model.Elements.Class import CIMClass
from cimpyorm.Model.Elements.Property import CIMProp, CIMProp_AlphaNumeric, CIMProp_Enumeration, CIMProp_Reference
from cimpyorm.Model.Elements.Datatype import CIMDT
from cimpyorm.backends import InMemory
from cimpyorm.Model.auxiliary import Base

log = get_logger(__name__)


class Schema:
    def __init__(self, dataset=None, version: str = "16", rdfs_path=None, profile_whitelist=None):
        """
        Initialize a Schema object, containing information about the schema elements.
        """
        self.g = None
        if not dataset:
            backend = InMemory()
            backend.reset()
            dataset = backend.ORM
        if not rdfs_path:
            rdfs_path = find_rdfs_path(version)
        if not rdfs_path:
            raise FileNotFoundError("Failed to find schema file. Please provide one.")
        self.rdfs_path = rdfs_path
        if profile_whitelist:
            profile_whitelist = self.parse_profile_whitelist(profile_whitelist)
            self.profiles = profile_whitelist
        self.schema_descriptions, profiles = merge_schema_descriptions(
            load_schema_descriptions(rdfs_path), profile_whitelist)
        log.info(f"Generating Schema backend.")
        try:
            elements = dataset.query(CIMClass).count()
        except OperationalError:
            elements = None
        if elements:
            # A schema is already present, so just load it instead of recreating
            self.session = dataset
            self.Element_classes = {c.__name__: c for c in
                                    [CIMPackage, CIMClass, CIMProp, CIMDT, CIMEnum, CIMEnumValue]}
            self.Elements = {c.__name__: {cim_class.name: cim_class for cim_class in dataset.query(c).all()}
                             for c in self.Element_classes.values()}
        else:
            self.session = dataset
            self.Element_classes = {c.__name__: c for c in
                                    [ElementMixin, CIMPackage, CIMClass, CIMProp, CIMDT, CIMEnum,
                                     CIMEnumValue]}
            self.Elements = {c.__name__: defaultdict(list) for c in self.Element_classes.values()}
            _Elements = []
            merged_nsmaps = dict(ChainMap(*(element.nsmap for element in
                                            self.schema_descriptions.values())))
            profiles = self._generate_profiles(profiles, merged_nsmaps, rdfs_path)
            self.session.add_all(profiles.values())
            xp = {"type_res": XPath(f"rdf:type/@rdf:resource", namespaces=merged_nsmaps),
                  "stype_res": XPath(f"cims:stereotype/@rdf:resource", namespaces=merged_nsmaps),
                  "stype_txt": XPath(f"cims:stereotype/text()", namespaces=merged_nsmaps)}
            for key, element in self.schema_descriptions.items():
                element.extract_types(xp)
                element.schema_type = element.get_type(xp)
            self._init_parser(merged_nsmaps)
            for short, full_uri in merged_nsmaps.items():
                _ns = CIMNamespace(short=short, full_name=full_uri)
                self.session.add(_ns)
            self._generate(profiles)

            self.session.commit()
            for _, Cat_Elements in self.Elements.items():
                self.session.add_all(Cat_Elements.values())
                self.session.commit()
            log.info(f"Schema generated")
            self._generate_ORM(dataset, profiles)
            dataset.schema = self

    def _generate_profiles(self, profiles, nsmap, rdfs_path=None):
        objects = {}
        if rdfs_path:
            filepath = os.path.abspath(os.path.join(rdfs_path, "Profile_Dependencies.json"))
            if os.path.isfile(filepath):
                with open(filepath, "r") as f:
                    raw = json.loads(f.read())
                    dependencies = defaultdict(dict)
                    for profile in raw["Profiles"]:
                        if "Mandatory" in profile:
                            dependencies[profile["Name"]]["Mandatory"] = profile["Mandatory"]
                        if "Optional" in profile:
                            dependencies[profile["Name"]]["Optional"] = profile["Optional"]
        for profile in profiles:
            if not profile.endswith("Profile"):
                raise ValueError("Invalid profile identifier.")
            uri_pattern = profile.replace("Profile", "Version") + ".entsoeURI"
            short_pattern = profile.replace("Profile", "Version") + ".shortName"
            uri_matches = {key: item for key, item in self.schema_descriptions.items()
                           if uri_pattern in key}
            short_matches = {key: item for key, item in self.schema_descriptions.items()
                             if short_pattern in key}
            URI = json.dumps(
                {key.split("#")[-1]: item.descriptions[profile].xpath(
                    "cims:isFixed/@rdfs:Literal", namespaces=nsmap)[0] for key, item in
                 uri_matches.items()}
            )
            _sm = list(short_matches)
            if not _sm:
                raise ValueError("Profile not defined.")
            if len(list(short_matches.values())) > 1:
                raise ValueError("Ambiguous profile shortName.")
            short = next(iter(short_matches.values())).descriptions[profile].xpath(
                "cims:isFixed/@rdfs:Literal", namespaces=nsmap)[0]

            _p = CIMProfile(name=profile, uri=URI, short=short)
            objects[profile] = _p
        for profile, object in objects.items():
            try:
                if "Mandatory" in dependencies[profile]:
                    object.mandatory_dependencies = [objects[dependency] for dependency in
                                                     dependencies[profile]["Mandatory"]]
            except KeyError:
                raise ValueError(f"An invalid composition of profiles was given. {profile} depends on"
                                 f" {dependencies[profile]['Mandatory']}, however, at least one of them was not " \
                                                                     "included in the whitelist.")
            if "Optional" in dependencies[profile]:
                object.optional_dependencies = [objects[dependency] for dependency in
                                                dependencies[profile]["Optional"] if dependency in objects]
        return objects

    def deduplicate(self):
        for se_type, objects in self.Elements.items():
            for key, values in objects.items():
                if len(values) > 1:
                    descrs = [value.schema_elements for value in values]
                    objects[key] = self.Element_classes[se_type](merge_descriptions(descrs),
                                                                 values[0].profile_name)
                else:
                    objects[key] = values[0]

    def get_inheritance_graph(self, profiles=None):
        """
        Determine the class inheritance hierarchy (class definition needs to adhere to strict inheritance hierarchy)
        :return: g - A networkx DiGraph of the class hierarchy, with a common ancestor __root__
        """
        # Determine class inheritance hierarchy (bfs on a directed graph)

        if not profiles:
            log.info(f"No profiles specified - using all profiles for ORM.")
        elif not isinstance(profiles, Iterable):
            profiles = (profiles,)

        g = DiGraph()
        g.add_node("__root__")
        class_list = list(self.session.query(CIMClass).all())
        classes = {}
        for c in class_list:
            if (c.namespace.short, c.name) in classes:
                raise ValueError("Duplicate class identity: %s_%s." % (c.namespace.short, c.name))
            classes[(c.namespace.short, c.name)] = c
        nodes = classes.keys()
        g.add_nodes_from(nodes)
        for key, instance in classes.items():
            if instance:
                parent = instance.parent
                if parent is None:
                    g.add_edge("__root__", key)
                else:
                    parent_key = (parent.namespace.short, parent.name)
                    g.add_edge(parent_key, key)
        return g, classes

    def _init_parser(self, nsmap):
        ElementMixin.nsmap = HDict(nsmap) # Set the nsmap on the Baseclass.
        for c in self.Element_classes.values():
            c._generateXPathMap()

    @property
    def model(self):
        for class_ in self.session.query(CIMClass).all():
            class_.p = Namespace(**class_.all_props)
        for enum_ in self.session.query(CIMEnum).all():
            enum_.v = Namespace(**{value.name: value for value in enum_.values})
        # The cim namespace is provided in top-level model as default namespace. Everything else
        # is hidden in separate Namespaces
        namespaces = {ns.short: ns for ns in self.session.query(CIMNamespace)}
        classes = {}
        for short, namespace in namespaces.items():
            classes[short] = \
                Namespace(**{c.name: c.class_ for c in
                             self.session.query(CIMClass).filter(CIMClass.namespace == namespace)})
        return Namespace(**classes["cim"].__dict__,
                         **classes,
                         **{"dt": Namespace(**{c.name: c for c in self.session.query(CIMDT).all()})},
                         **{"classes": Namespace(**{c.name: c for c in self.session.query(CIMClass).all()})},
                         **{"enum": Namespace(**{c.name: c for c in self.session.query(
                             CIMEnum).all()})},
                         **{"schema": self})

    def get_classes(self):
        return {c.name: c.class_ for c in self.session.query(CIMClass).all()}

    def _generate(self, profiles):
        _Elements = self.Elements
        postponed = []
        insertables = []
        for key, element in self.schema_descriptions.items():
            if not element.schema_type.postpone:
                type_name = element.schema_type.name
                try:
                    obj = self.Element_classes[type_name](element)
                    _Elements[type_name][obj.u_key] = obj
                    obj.used_in = [profiles[_p] for _p in element.get_all_profiles()]
                    if isinstance(obj, CIMClass):
                        element_profile = element.get_profile()
                        obj.defined_in = element_profile
                except KeyError:
                    log.warning(f"Unknown element: {element}.")

            else:
                postponed.append(element)
        for element in postponed:
            type_res = element.type_res
            if type_res and type_res[0].endswith("#Property"):
                obj = CIMProp(element)
                domain = obj._get_domain()
                if se_ref(domain[1], domain[0]) in _Elements["CIMDT"].keys():
                    dt = _Elements["CIMDT"][se_ref(domain[1], domain[0])]
                    if obj.name == "unit":
                        dt.set_unit(element.descriptions, type="nominator")
                    elif obj.name == "value":
                        dt.set_datatype(element.descriptions)
                    elif obj.name == "multiplier":
                        dt.set_multiplier(element.descriptions, type="nominator")
                    elif obj.name == "denominatorUnit":
                        dt.set_unit(element.descriptions, type="denominator")
                    elif obj.name == "denominatorMultiplier":
                        dt.set_multiplier(element.descriptions, type="denominator")
                    else:
                        raise TypeError
                else:
                    if not obj.range_name:
                        obj = CIMProp_AlphaNumeric(element)
                    else:
                        range = obj._get_range()
                        key = se_ref(range[1], obj.namespace_name)
                        if key in _Elements["CIMEnum"]:
                            obj = CIMProp_Enumeration(element)
                        else:
                            obj = CIMProp_Reference(element)
                    _Elements["CIMProp"][obj.u_key] = obj
                    obj.defined_in = element.get_profile()
                    # ToDo: Find out why using "allowed_in" causes UNIQUE constraint errors on
                    #  CIMProp
                    # obj.allowed_in = [profiles[_p] for _p in element.get_all_profiles()]
                    for profile in element.get_all_profiles():
                        insertables.append(
                            prop_used_in.insert().values(
                                profile_name=profile,
                                prop_namespace=obj.namespace_name,
                                prop_name=obj.name,
                                prop_cls_namespace=obj.cls_namespace,
                                prop_cls_name=obj.cls_name))
                continue
            obj = CIMEnumValue(element)
            enum = obj._get_enum()
            if se_ref(enum[1], enum[0]) in _Elements["CIMEnum"]:
                _Elements["CIMEnumValue"][obj.u_key] = obj
            else:
                name = enum[1]
                _notfound = True
                for key, enum in _Elements["CIMEnum"].items():
                    if enum.name == name:
                        obj.namespace_name = key.namespace_name
                        obj.enum_namespace = key.namespace_name
                        _Elements["CIMEnumValue"][obj.u_key] = obj
                        _notfound=False
                        break
                if _notfound:
                    log.warning(f"Failed to identify purpose for {type_res}")
        for insertable in insertables:
            self.session.execute(insertable)

    @property
    def map(self):
        if not self.g:
            g = DiGraph()
            classnames = [_[0] for _ in self.session.query(CIMClass.name).all()]
            classes = self.session.query(CIMClass).all()
            enums = self.session.query(CIMEnum).all()
            enumnames = [_[0] for _ in self.session.query(CIMEnum.name).all()]
            propnames = [_[0] for _ in self.session.query(CIMProp.name).all()]
            g.add_nodes_from(classnames)
            g.add_nodes_from(enumnames)
            g.add_nodes_from(propnames)

            for node in classes + enums:
                try:
                    for prop in node.all_props.values():
                        if prop.range:
                            g.add_edge(node.name, prop.range.name, label=prop.label)
                        else:
                            g.add_edge(node.name, prop.name, label=prop.label)
                except AttributeError:
                    pass
            self.g = g
        return self.g

    def path(self, source, destination):
        from fuzzyset import FuzzySet
        if source == destination:
            return
        fuzz = FuzzySet(self.map.nodes)
        if source not in self.map.nodes:
            source = fuzzymatch(fuzz, source)
        if destination not in self.map.nodes:
            destination = fuzzymatch(fuzz, destination)
        try:
            path = nx.shortest_path(self.map, source, destination)
        except NetworkXNoPath:
            log.error(f"No path between {source.name} and {destination.name}.")
            return
        way = []
        for iter in range(1, len(path)):
            way.append(self.map.edges[path[iter-1], path[iter]]["label"])
        return way

    def deduplicate_schema_elements(self, _Elements, profile):
        for Category, CatElements in _Elements.items():
            log.debug(f"Merging {Category}.")
            for NodeName, NodeElements in CatElements.items():
                CatElements[NodeName] = self.Element_classes[Category](
                    merge_descriptions([e.schema_elements for e in NodeElements]), profile)
            _Elements[Category] = dict(CatElements)
        return _Elements

    def flatten(self):
        result = self.Elements
        for _profile in self.Elements:
            for Cat, Items in _profile.items():
                for Item, Value in Items.items():
                    [result[Cat].append(v) for v in Value]

    def _generate_ORM(self, session, profiles=None):
        # Fixme: 20 seconds
        hierarchy = self.class_hierarchy(profiles)
        try:
            for c in hierarchy:
                c.init_type(Base)
        except InvalidRequestError as ex:
            ex
        session.commit()
        session.flush()
        namespaces = session.query(CIMNamespace.short, CIMNamespace.full_name).all()
        nsmap = {k: v for k, v in namespaces}
        for c in hierarchy:
            c.generate(nsmap)
        log.info(f"Generated {len(hierarchy)} classes")

    def class_hierarchy(self, profiles=None, mode="bfs"):
        g, classes = self.get_inheritance_graph(profiles)
        if mode == "dfs":
            nodes = list(dfs_tree(g, "__root__"))
        else:
            nodes = list(bfs_tree(g, "__root__"))
        nodes.remove("__root__")
        return [classes[node] for node in nodes]

    def parse_profile_whitelist(self, profile_whitelist):
        filepath = os.path.abspath(os.path.join(self.rdfs_path, "Profile_Dependencies.json"))
        if os.path.isfile(filepath):
            with open(filepath, "r") as f:
                raw = json.loads(f.read())
                aliases = {profile["short"]: profile["Name"] for profile in raw["Profiles"]}
        try:
            profiles = set((aliases[profile] if profile not in aliases.values() else profile for profile in
                            profile_whitelist))
        except KeyError:
            raise ValueError(f"Unknown Profile shortName provided")
        return profiles


class SchemaDescription:
    def __init__(self, tree):
        self.tree = tree
        self.root = self.tree.getroot()
        self.nsmap = self.root.nsmap
        self.associated_profile = str(self._get_profile())

    @classmethod
    def from_file(cls, path):
        return cls(parse(path))

    def _get_profile(self):
        first_element = self.root[0]
        if not first_element.attrib.values()[0].endswith("Profile"):
            raise ValueError("Profile element not found in schema description (should be position 1).")
        return first_element.xpath(f"rdfs:label/text()", namespaces=self.nsmap)[0]


class SchemaElement:
    def __init__(self, descriptions=None):
        self.name = None
        self._types = Namespace()
        self.type_res = None
        self.stype_res = None
        self.stype_txt = None
        self.nsmap = {}
        self.schema_type = None
        if not descriptions:
            self.descriptions = {}
        else:
            self.descriptions = descriptions
            for description in descriptions:
                self.nsmap.update(description.nsmap)

    def get_profile(self):
        candidates = set([k for k, v in self._types.stype_res.items()
                          if v == "http://iec.ch/TC57/NonStandard/UML#concrete"])
        if not candidates:
            candidates = self.descriptions.keys()
        if len(candidates) == 1:
            return next(iter(candidates))
        elif len(set((c.replace("Boundary", "") for c in candidates))) == 1:
            return next(iter(candidates)).replace("Boundary", "")
        else:
            candidates
            log.warning(f"Multiple profiles found for {self.name}. Defaulting to EquipmentProfile.")
            return "EquipmentProfile"

    def get_all_profiles(self):
        return tuple(self.descriptions.keys())

    def update(self, profile, description):
        if not self.name:
            self.name = description.values()[0]
        elif not self.name == description.values()[0]:
            raise ValueError("Ambiguous SchemaElement.")
        if profile not in self.descriptions:
            self.descriptions.update({profile: description})
        else:
            self.descriptions[profile].extend(description)
        for k, v in description.nsmap.items():
            if k in self.nsmap and not v == self.nsmap[k]:
                raise ValueError("Ambiguous namespace definition.")
            else:
                self.nsmap[k] = v

    def extract_types(self, xp):
        self._types.type_res = self._value(xp["type_res"])
        self._types.stype_res = self._value(xp["stype_res"])
        self._types.stype_txt = self._value(xp["stype_txt"])
        self.type_res = tuple(set(elements for elements in self._types.type_res.values()))
        self.stype_res = tuple(set(elements for elements in self._types.stype_res.values()))
        self.stype_txt = tuple(set(elements for elements in self._types.stype_txt.values()))

    def get_type(self, xp):
        type_res = self.type_res
        stype_res = self.stype_res
        stype_txt = self.stype_txt
        if len(type_res) > 1:
            raise ValueError
        if len(stype_res) > 1 or len(stype_txt) > 1:
            type_res
        if type_res and any(v.endswith("#Class") for v in type_res):
            # Element is a class object
            if stype_res and stype_res[0].endswith("#enumeration"):
                # Enumeration
                return se_type("CIMEnum", False)
            elif stype_txt and "CIMDatatype" in stype_txt or "Primitive" in stype_txt:
                # Datatype
                return se_type("CIMDT", False)
            else:
                # Proper class
                return se_type("CIMClass", False)
        elif type_res and any(v.endswith("#Property") for v in type_res):
            # Properties can be several types of objects. We postpone, so we can determine the
            # type later.
            return se_type("Uncertain", True)
        elif type_res and any(v.endswith("#ClassCategory") for v in type_res):
            return se_type("CIMPackage", False)
        else:
            return se_type("Unknown", True)

    def _value(self, xp):
        res = {profile: set(xp(element)) for profile, element in self.descriptions.items() if xp(
            element)}
        for key, value in res.items():
            if len(value) > 1:
                value
            res[key] = value.pop()
        return res

    def xpath(self, xpath_expr):
        return apply_xpath(xpath_expr, self.descriptions)


def load_schema_descriptions(path):
    """
    Loads the schema descriptions
    :param path:
    :return:
    """
    return [SchemaDescription.from_file(os.path.join(path, file)) for file in os.listdir(path) if
            file.endswith(".rdf")]


def merge_schema_descriptions(descriptions, profile_whitelist=None):
    _elements = defaultdict(SchemaElement)
    if not profile_whitelist:
        profiles = set((d.associated_profile for d in descriptions))
    else:
        profiles = set(profile_whitelist)
    for description in descriptions:
        if description.associated_profile in profiles:
            for child in description.root:
                xml_key = child.values()[0]
                _elements[xml_key].update(description.associated_profile, child)
    _elements = dict(_elements)
    return _elements, profiles


def merge_nsmaps(nsmaps):
    merged = nsmaps[0]
    for nsmap in nsmaps[1:]:
        for k, v in nsmap.items():
            if k in merged and v != merged[k]:
                log.error("Incompatible namespaces in nsmaps")
            merged[k] = v
    return merged


def fuzzymatch(set, value):
    result = set.get(value)
    if result and result[0][0]>0.2:
        log.warning(f"Did you mean {result[0][1]} (matched from {value})?")
        return result[0][1]
    else:
        return None


class SchemaInfo(Base):
    __tablename__ = "SchemaInfo"
    namespaces = Column(TEXT)
    id = Column(Integer, primary_key=True, autoincrement=True)

    def __init__(self, nsmap):
        """
        Initialize SchemaInfo object
        :param source_file: Path to the file containing the model data
        """
        self.namespaces = json.dumps(nsmap)

    @property
    def nsmap(self):
        """
        Return the source's nsmap
        :return: dict - The source's nsmap
        """
        nsmap = json.loads(self.namespaces)
        return nsmap
