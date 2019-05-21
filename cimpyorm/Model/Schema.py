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

import json
from argparse import Namespace
from collections import defaultdict

import lxml.etree as et
from lxml.etree import XPath
import networkx as nx
from networkx import DiGraph, bfs_tree, dfs_tree
from networkx.exception import NetworkXNoPath
from sqlalchemy import Column, TEXT, Integer
from sqlalchemy.exc import InvalidRequestError

from cimpyorm.auxiliary import log, merge, HDict, merge_descriptions, find_rdfs_path
import cimpyorm.Model.auxiliary as aux
from cimpyorm.Model.Elements import CIMPackage, CIMClass, CIMProp, CIMDT, CIMEnum, CIMEnumValue, \
    CIMDTUnit, CIMDTValue, CIMDTMultiplier, CIMDTDenominatorUnit, SchemaElement, CIMDTProperty, \
    CIMDTDenominatorMultiplier
from cimpyorm.backends import InMemory


class Schema:
    def __init__(self, session=None, version: str = "16"):
        """
        Initialize a Backend object, containing information about the schema elements
        :param file_or_tree: The schema file or a parsed root
        """
        self.g = None
        if not session:
            backend = InMemory()
            backend.reset()
            session = backend.session
        rdfs_path = find_rdfs_path(version)
        if not rdfs_path:
            raise FileNotFoundError("Failed to find schema file. Please provide one.")
        tree = merge(rdfs_path)
        log.info(f"Dynamic code generation.")
        if session.query(SchemaElement).count():
            # A schema is already present, so just load it instead of recreating
            self.session = session
            self.Element_classes = {c.__name__: c for c in
                                    [CIMPackage, CIMClass, CIMProp, CIMDT, CIMEnum, CIMEnumValue, CIMDTUnit,
                                     CIMDTValue, CIMDTMultiplier, CIMDTDenominatorUnit, CIMDTDenominatorMultiplier]}
            self.Elements = {c.__name__: {cim_class.name: cim_class for cim_class in session.query(c).all()}
                             for c in self.Element_classes.values()}
        else:
            self.session = session
            if isinstance(tree, type(et.ElementTree())):
                self.file = None
                self.root = tree.getroot()
            else:
                self.file = tree
                self.root = et.parse(tree).getroot()
            self.Element_classes = {c.__name__: c for c in
                                    [CIMPackage, CIMClass, CIMProp, CIMDT, CIMEnum, CIMEnumValue, CIMDTUnit,
                                     CIMDTValue, CIMDTMultiplier, CIMDTDenominatorUnit, CIMDTDenominatorMultiplier]}
            self.Elements = {c.__name__: defaultdict(list) for c in self.Element_classes.values()}
            self._init_parser()
            self._generate()
            for _, Cat_Elements in self.Elements.items():
                self.session.add_all(list(Cat_Elements.values()))
                self.session.commit()
            log.debug(f"Backend generated")
            session.add(SchemaInfo(self.root.nsmap))
            self.init_model(session)

    @property
    def inheritance_graph(self):
        """
        Determine the class inheritance hierarchy (class definition needs to adhere to strict inheritance hierarchy)
        :param classes: dict of CIMClass objects
        :return: g - A networkx DiGraph of the class hierarchy, with a common ancestor __root__
        """
        # Determine class inheritance hierarchy (bfs on a directed graph)
        g = DiGraph()
        g.add_node("__root__")
        class_list = list(self.session.query(CIMClass).all())
        while class_list:
            for element in class_list:
                if element:
                    parent = element.parent
                    if not parent:
                        g.add_edge("__root__", element)
                    else:
                        g.add_edge(parent, element)
                class_list.remove(element)
        return g

    def _init_parser(self):
        SchemaElement.nsmap = HDict(self.root.nsmap)
        for c in self.Element_classes.values():
            c._generateXPathMap()

    @staticmethod
    def _isclass(type_res):
        return type_res and type_res[0].endswith("#Class")

    @staticmethod
    def _isenum(stype_res):
        return stype_res and stype_res[0].endswith("#enumeration")

    @staticmethod
    def _isdt(stype_txt):
        return stype_txt and stype_txt[0] in ["CIMDatatype", "Primitive"]

    @staticmethod
    def _isprop(type_res):
        return type_res and type_res[0].endswith("#Property")

    @staticmethod
    def _ispackage(type_res):
        return type_res and type_res[0].endswith("#ClassCategory")

    @property
    def model(self):
        for class_ in self.session.query(CIMClass).all():
            class_.p = Namespace(**class_.all_props)
        for enum_ in self.session.query(CIMEnum).all():
            enum_.v = Namespace(**{value.label: value for value in enum_.values})
        return Namespace(**{c.name: c.class_ for c in self.session.query(CIMClass).all()},
                         **{"dt": Namespace(**{c.name: c for c in self.session.query(CIMDT).all()})},
                         **{"classes": Namespace(**{c.name: c for c in self.session.query(CIMClass).all()})},
                         **{"enum": Namespace(**{c.name: c for c in self.session.query(
                             CIMEnum).all()})},
                         **{"schema": self})

    def _generate(self):
        xp_type_res = XPath(f"rdf:type/@rdf:resource", namespaces=self.root.nsmap)
        xp_stype_res = XPath(f"cims:stereotype/@rdf:resource", namespaces=self.root.nsmap)
        xp_stype_txt = XPath(f"cims:stereotype/text()", namespaces=self.root.nsmap)
        postponed = []
        for element in self.root:
            type_res = xp_type_res(element)
            stype_res = xp_stype_res(element)
            stype_txt = xp_stype_txt(element)
            if Schema._isclass(type_res):
                if Schema._isenum(stype_res):
                    obj = CIMEnum(element)
                    self.Elements["CIMEnum"][obj.name].append(obj)
                elif Schema._isdt(stype_txt):
                    obj = CIMDT(element)
                    self.Elements["CIMDT"][obj.name].append(obj)
                else:
                    obj = CIMClass(element)
                    self.Elements["CIMClass"][obj.name].append(obj)
            elif Schema._isprop(type_res):
                postponed.append(element)
            elif Schema._ispackage(type_res):
                obj = CIMPackage(element)
                self.Elements["CIMPackage"][obj.name].append(obj)
            elif type_res:
                postponed.append(element)
            else:
                obj = SchemaElement(element)
                log.warning(f"Element skipped: {obj.name}")
        for element in postponed:
            type_res = xp_type_res(element)
            if Schema._isprop(type_res):
                obj = CIMProp(element)
                if obj._domain in self.Elements["CIMDT"].keys():
                    if obj.name.endswith(".unit"):
                        obj = CIMDTUnit(element)
                        self.Elements["CIMDTUnit"][obj.name].append(obj)
                    elif obj.name.endswith(".value"):
                        obj = CIMDTValue(element)
                        self.Elements["CIMDTValue"][obj.name].append(obj)
                    elif obj.name.endswith(".multiplier"):
                        obj = CIMDTMultiplier(element)
                        self.Elements["CIMDTMultiplier"][obj.name].append(obj)
                    elif obj.name.endswith(".denominatorUnit"):
                        obj = CIMDTDenominatorUnit(element)
                        self.Elements["CIMDTDenominatorUnit"][obj.name].append(obj)
                    elif obj.name.endswith(".denominatorMultiplier"):
                        obj = CIMDTDenominatorMultiplier(element)
                        self.Elements["CIMDTDenominatorMultiplier"][obj.name].append(obj)
                    else:
                        obj = CIMDTProperty(element)
                        self.Elements["CIMDTProperty"][obj.name].append(obj)
                else:
                    self.Elements["CIMProp"][obj.name].append(obj)
                continue
            obj = CIMEnumValue(element)
            if obj._enum_name and obj._enum_name in self.Elements["CIMEnum"].keys():
                self.Elements["CIMEnumValue"][obj.name].append(obj)
            else:
                log.debug(f"Failed to identify purpose for {type_res}")
        self._merge_elements()
        for key, value in self.Elements.items():
            if value:
                log.debug(f"Generated {len(value)} {key}.")

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

    def _merge_elements(self):
        for Category, CatElements in self.Elements.items():
            log.debug(f"Merging {Category}.")
            for NodeName, NodeElements in CatElements.items():
                CatElements[NodeName] = self.Element_classes[Category](
                    merge_descriptions([e.description for e in NodeElements]))
            self.Elements[Category] = dict(CatElements)

    def init_model(self, session):
        additionalNodes = self.class_hierarchy()

        hierarchy = additionalNodes
        try:
            for c in hierarchy:
                c.init_type(aux.Base)
        except InvalidRequestError:
            pass
        session.commit()
        session.flush()
        nsmap = session.query(SchemaInfo).one().nsmap
        for c in hierarchy:
            c.generate(nsmap)
        log.info(f"Generated {len(hierarchy)} classes")

    def class_hierarchy(self, mode="bfs"):
        if mode == "dfs":
            nodes = list(dfs_tree(self.inheritance_graph, "__root__"))
        else:
            nodes = list(bfs_tree(self.inheritance_graph, "__root__"))
        nodes.remove("__root__")
        return nodes


def fuzzymatch(set, value):
    result = set.get(value)
    if result and result[0][0]>0.2:
        log.warning(f"Did you mean {result[0][1]} (matched from {value})?")
        return result[0][1]
    else:
        return None


class SchemaInfo(aux.Base):
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
