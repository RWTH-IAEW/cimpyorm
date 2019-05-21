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
from functools import lru_cache
from typing import Collection, Iterable
from pathlib import Path
import configparser
import logging
from zipfile import ZipFile
from itertools import chain


class HDict(dict):
    """Provide a hashable dict for use as cache key"""
    def __hash__(self):
        return hash(frozenset(self.items()))


def chunks(l: Collection, n: int) -> Iterable:
    """
    Iteratively yield from an iterable at most n elements.
    :param l: The iterable to yield from.
    :param n: The maximum number of elements
    :return: Yield elements from the iterable.
    """
    for i in range(0, len(l), n):
        yield l[i:i+n]


class CustomFormatter(logging.Formatter):
    """
    Elapsed time logging formatter.
    """
    def formatTime(self, record, datefmt=None):
        return f"{round(record.relativeCreated/1000)}." \
               f"{round(record.relativeCreated%1000)}"


log = logging.getLogger("cim_orm")
if not log.handlers:
    log.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    log.addHandler(handler)
    formatter = CustomFormatter(fmt='T+%(asctime)10ss:%(levelname)8s: %(message)s')
    handler.setFormatter(formatter)
    log.debug("Logger configured.")

CONFIG = configparser.ConfigParser()
# Set default paths
CONFIG["Paths"] = {"PACKAGEROOT": Path(os.path.abspath(__file__)).parent,
                   "TESTROOT": os.path.join(Path(os.path.abspath(__file__)).parent, "Test"),
                   "CONFIGPATH": os.path.join(Path(os.path.abspath(__file__)).parent, "config.ini")}

_TESTROOT = CONFIG["Paths"]["TESTROOT"]
_PACKAGEROOT = CONFIG["Paths"]["PACKAGEROOT"]
_CONFIGPATH = CONFIG["Paths"]["CONFIGPATH"]


def get_path(identifier: str) -> str:
    """
    Get the requested path from the package config.
    :param identifier: Path-type identifier.
    :return:
    """
    config = configparser.ConfigParser()
    config.read(_CONFIGPATH)
    return config["Paths"][identifier]


def merge(source_path):
    """
    Merges several ElementTrees into one.

    :return: Merged Elementtree
    """
    from lxml import etree as et
    path = source_path
    files = parseable_files(path)
    base = et.parse(files[0])
    root = base.getroot()
    nsmap = root.nsmap
    for file in files[1:]:
        tree = et.parse(file)
        for key, value in tree.getroot().nsmap.items():
            if key in nsmap and value != nsmap[key]:
                log.error("Incompatible namespaces in schema files")
            nsmap[key] = value
        for child in tree.getroot():
            root.append(child)
    tree = et.ElementTree(root)
    et.cleanup_namespaces(tree, top_nsmap=nsmap, keep_ns_prefixes=nsmap.keys())
    return tree


def parseable_files(path):
    """
    Identify the parseable files within a directory (.xml/.rdf)
    :param path: path to the directory
    :return: list of files
    """
    if path.endswith(".rdf") or path.endswith(".xml"):
        files = [path]
    elif path.endswith(".zip"):
        dir_ = ZipFile(path, "r")
        files = [dir_.open(name) for name in dir_.namelist() if name.endswith(
            ".xml") or name.endswith(".rdf")]
    else:
        files = os.listdir(os.path.abspath(path))
        files = [os.path.join(path, file) for file in files if
                 file.endswith(".xml") or file.endswith(".rdf")]
        if not files:
            # There are no xml files in the folder - assume the first .zip
            # is the zipped CIM
            files = [os.path.join(path, file) for file in os.listdir(path) if
                     file.endswith(".zip") or file.endswith(".rdf")]
            dir_ = ZipFile(files[0])
            files = [dir_.open(name) for name in dir_.namelist() if name.endswith(
                ".xml") or name.endswith(".rdf")]
    return files


@lru_cache()
def shorten_namespace(elements, nsmap):
    """
    Map a list of XML tag class names on the internal classes (e.g. with shortened namespaces)
    :param classes: list of XML tags
    :param nsmap: XML nsmap
    :return: List of mapped names
    """
    names = []
    _islist = True
    if not isinstance(elements, (list, frozenset)):
        elements = [elements]
        _islist = False
    for el in elements:
        for key, value in nsmap.items():
            if value in el:
                if key == "cim":
                    name = el.split(value)[-1]
                    name = name[1:] if name.startswith("}") else name
                elif "{"+value+"}" in el:
                    name = el.replace("{"+value+"}", key+"_")
                else:
                    name = el.replace(value, key+"_")
                names.append(name)
        if el.startswith("#"):
            names.append(el.split("#")[-1])
    if not _islist and len(names) == 1:
        names = names[0]
    return names


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
