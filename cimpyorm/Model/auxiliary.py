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

import os
from itertools import chain

from sqlalchemy.ext.declarative import declarative_base

from cimpyorm import get_path, log

Base = declarative_base()


class HDict(dict):
    """Provide a hashable dict for use as cache key"""
    def __hash__(self):
        return hash(frozenset(self.items()))


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]


def merge_descriptions(descriptions):
    """
    Returns the descriptions for a CIM class merged into only one description

    :param descriptions: Iterable of the descriptions
    :return: Result of the merge
    """
    if isinstance(descriptions, list):
        description = descriptions[0]
        # pylint: disable=expression-not-assigned
        [description.append(value) for value in list(chain(*[list(descr) for descr in descriptions]))]
    else:
        description = descriptions
    return description


def find_rdfs_path(version):
    """
    Attempt to identify which schema to use from the model file header.
    :param version: The CIM version.
    :return: Path to the schema files on local file system
    """
    if version:
        log.info(f"Using CIM Version {version}.")
    else:
        raise ValueError(f"Failed to determine CIM Version")
    if len(version) > 2:
        raise ValueError(f"Unexpected CIM Version (v={version}).")
    try:
        rdfs_path = os.path.join(get_path("SCHEMAROOT"), f"CIM{version}")
    except KeyError:
        log.critical(f"Schema not defined.")
        raise RuntimeError(f"Couldn't find CIM schemata. "
                           f"Please configure a schema repository using cimpyorm.configure.")
    if not os.path.isdir(rdfs_path):
        raise NotImplementedError(f"Unknown CIM Version for (v={version}). Add to "
                                  f"schemata")
    return rdfs_path


def map_enum(elements, nsmap):
    """
    Map a list of XML tag class names on the internal classes (e.g. with shortened namespaces)
    :param classes: list of XML tags
    :param nsmap: XML nsmap
    :return: List of mapped names
    """
    if not isinstance(elements, list):
        name = None
        el = elements
        for key, value in nsmap.items():
            if value in el:
                if key == "cim":
                    name = el.split(value)[-1]
                else:
                    name = el.replace(value, key+"_")
        if el.startswith("#"):
            name = el.split("#")[-1]
        res = name
    else:
        names = []
        for el in elements:
            for key, value in nsmap.items():
                if value in el:
                    if key == "cim":
                        names.append(el.split(value)[-1])
                    else:
                        names.append(el.replace(value, key+"_"))
            if el.startswith("#"):
                names.append(el.split("#")[-1])
        res = names
    return res


def prefix_ns(func):
    """
    Prefixes a property return value with the elements xml-namespace (if its not the default namespace "cim").

    Creates unique labels for properties and classes.
    """
    def wrapper(obj):
        """
        :param obj: Object that implements the namespace property (E.g. CIMClass/CIMProp)
        :return: Representation with substituted namespace
        """
        s = func(obj)
        res = []
        if s and isinstance(s, list):
            for element in s:
                if element.startswith("#"):
                    element = "".join(element.split("#")[1:])
                for key, value in obj.nsmap.items():
                    if value in element:
                        element = element.replace(value, key+"_")
                res.append(element)
        elif s:
            if s.startswith("#"):
                s = "".join(s.split("#")[1:])
            for key, value in obj.nsmap.items():
                if value in s:
                    s = s.replace(value, key + "_")
            res = s
        else:
            res = None
        return res
    return wrapper
