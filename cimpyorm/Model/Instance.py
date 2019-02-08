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
from networkx import DiGraph, bfs_tree
from sqlalchemy import Column, TEXT, Integer
from sqlalchemy.exc import InvalidRequestError

from cimpyorm import log
import cimpyorm.Model.auxiliary as aux
from cimpyorm.Model.Elements import CIMPackage, CIMClass, CIMProp, CIMDT, CIMEnum, CIMEnumValue, \
            CIMDTUnit, CIMDTValue, CIMDTMultiplier, CIMDTDenominatorUnit, SchemaElement, CIMDTProperty, \
            CIMDTDenominatorMultiplier


class Schema:
    def __init__(self, session, file_or_tree=None):
        """
        Initialize a Backend object, containing information about the schema elements
        :param file_or_tree: The schema file or a parsed root
        """
        if not file_or_tree:
            self.session = session
            self._Element_classes = {c.__name__: c for c in
                                     [CIMPackage, CIMClass, CIMProp, CIMDT, CIMEnum, CIMEnumValue, CIMDTUnit,
                                      CIMDTValue, CIMDTMultiplier, CIMDTDenominatorUnit, CIMDTDenominatorMultiplier]}
            self.Elements = {c.__name__: {cim_class.name: cim_class for cim_class in session.query(c).all()}
                             for c in self._Element_classes.values()}
        else:
            self.session = session
            if isinstance(file_or_tree, type(et.ElementTree())):
                self.file = None
                self.root = file_or_tree.getroot()
            else:
                self.file = file_or_tree
                self.root = et.parse(file_or_tree).getroot()
            self._Element_classes = {c.__name__: c for c in
                                     [CIMPackage, CIMClass, CIMProp, CIMDT, CIMEnum, CIMEnumValue, CIMDTUnit,
                                      CIMDTValue, CIMDTMultiplier, CIMDTDenominatorUnit, CIMDTDenominatorMultiplier]}
            self.Elements = {c.__name__: defaultdict(list) for c in self._Element_classes.values()}
            self._init_parser()
            self._generate()
            for _, Cat_Elements in self.Elements.items():
                self.session.add_all(list(Cat_Elements.values()))
                self.session.commit()
            log.debug(f"Backend generated")

    def create_inheritance_graph(self):
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
        SchemaElement.nsmap = self.root.nsmap
        for c in self._Element_classes.values():
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
                         **{"enum": Namespace(**{c.name: c for c in self.session.query(CIMEnum).all()})})

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

    def _merge_elements(self):
        for Category, CatElements in self.Elements.items():
            log.debug(f"Merging {Category}.")
            for NodeName, NodeElements in CatElements.items():
                CatElements[NodeName] = self._Element_classes[Category](
                    aux.merge_descriptions([e.description for e in NodeElements]))
            self.Elements[Category] = dict(CatElements)

    def init_model(self, session):
        g = self.create_inheritance_graph()

        additionalNodes = list(bfs_tree(g, "__root__"))
        additionalNodes.remove("__root__")

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
