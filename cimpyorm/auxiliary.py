#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

import configparser
import logging
import os
from functools import lru_cache
from itertools import chain
from logging.handlers import RotatingFileHandler
from pathlib import Path
from shutil import copytree, copy
from typing import Collection, Iterable
from zipfile import ZipFile
from argparse import Namespace

import pandas as pd
from sqlalchemy.orm import Session as SA_Session
from sqlalchemy import func
from tabulate import tabulate
# XPath yields low-severity positives in bandit. The XPath expressions are evaluated on securely parsed
# (defusedxml)  and not parametrized dynamically. Therefore we ignore these warnings.
from lxml.etree import XPath    # nosec

DEFAULTS = Namespace(Namespace="")


class Dataset(SA_Session):
    """
    The Dataset Class holds the CIM data as well as a reference to the Schema that was used.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = None
        self.mas = None
        self.scenario_time = None

    def get_stats(self, fmt="psql"):
        print("---- CLASSES ----")
        print(tabulate(self._count_classes(), headers="keys", showindex=False, tablefmt=fmt))

    def _count_classes(self):
        # Import during call to avoid circular reference
        from cimpyorm.Model.Elements import CIMClass
        # Determine Schema roots (e.g IdentifiedObject and so on)
        roots = [root.class_ for root in
                 self.query(CIMClass).filter(
                     CIMClass.parent_name == None, CIMClass.parent_namespace == None
                 ).all()]
        r = [self.query(root.type_, func.count(root.type_)).group_by(
            root.type_).all() for root in roots]
        r = chain(*r)
        r = ((*name.split("_"), count) for name, count in r)
        df = pd.DataFrame.from_records(r, columns=("Namespace", "Name", "Count"))
        df = df.append({"Namespace": "---",
                        "Name": " - SUM - ",
                        "Count": df.Count.sum()}, ignore_index=True)
        del r
        return df

    def get_objects(self):
        # Import during call to avoid circular reference
        from cimpyorm.Model.Elements import CIMClass
        # Determine Schema roots (e.g IdentifiedObject and so on)
        roots = [root.class_ for root in
                 self.query(CIMClass).filter(CIMClass.parent == None).all()]
        return chain(*(self.query(root).all() for root in roots))

    @property
    def objects(self):
        from cimpyorm.Model.Elements import CIMClass
        # Determine Schema roots (e.g IdentifiedObject and so on)
        roots = [root.class_ for root in
                 self.query(CIMClass).filter(CIMClass.parent == None).all()]
        return sum([self.query(root).count() for root in roots])


class HDict(dict):
    """Provide a hashable dict for use as cache key"""
    def __hash__(self):
        return hash(frozenset(self.items()))

@lru_cache()
def invert_dict(_d):
    return {value: key for key, value in _d}


def chunks(l: Collection, n: int) -> Iterable:
    """
    Iteratively yield from an iterable at most n elements.
    :param l: The iterable to yield from.
    :param n: The maximum number of elements
    :return: Yield elements from the iterable.
    """
    for i in range(0, len(l), n):
        yield l[i:i+n]

#
# def add_schema(version_number, path):
#     config = configparser.ConfigParser()
#     config.read(get_path("CONFIGPATH"))
#     if f"CIM{version_number}" not in os.listdir(config["Paths"]["Schemaroot"]):
#         dst = os.path.join(config["Paths"]["Schemaroot"], f"CIM{version_number}")
#         if os.path.isfile(path):
#             os.makedirs(dst)
#             copy(path, dst)
#         elif os.path.isdir(path):
#             copytree(path, dst)
#     else:
#         raise FileExistsError(r"A schema for this version number already exists")


class CustomFormatter(logging.Formatter):
    """
    Elapsed time logging formatter.
    """
    def formatTime(self, record, datefmt=None):
        return f"{round(record.relativeCreated/1000)}." \
               f"{round(record.relativeCreated%1000)}"


def get_console_handler():
    handler = logging.StreamHandler()
    # formatter = CustomFormatter(fmt='T+%(asctime)10ss:%(levelname)8s: %(name)s - %(message)s')
    formatter = logging.Formatter("%(asctime)s:%(levelname)8s:%(name)s - %(message)s")
    handler.setFormatter(formatter)
    return handler


def get_file_handler(filename):
    """
    Default FileHandler
    :param filename:
    :return:
    """
    try:
        os.remove(filename)
    except FileNotFoundError:
        pass
    handler = RotatingFileHandler(filename, mode="w", maxBytes=4e5, backupCount=1)
    # formatter = CustomFormatter(fmt='T+%(asctime)10ss:%(levelname)8s: %(name)s - %(message)s')
    formatter = logging.Formatter("%(asctime)s:%(levelname)8s:%(name)s - %(message)s")
    handler.setFormatter(formatter)
    return handler


def get_logger(name):
    logger = logging.getLogger(name)
    logger.propagate = True
    return logger


log = get_logger(__name__)

CONFIG = configparser.ConfigParser()
# Set default paths
CONFIG["Paths"] = {"PACKAGEROOT": Path(os.path.abspath(__file__)).parent,
                   "TESTROOT": os.path.join(Path(os.path.abspath(__file__)).parent, "Test"),
                   "CONFIGPATH": os.path.join(Path(os.path.abspath(__file__)).parent,
                                              "config.ini"),
                   "SCHEMAROOT": os.path.join(Path(os.path.abspath(__file__)).parent,
                                              "res", "schemata"),
                   "DATASETROOT": os.path.join(Path(os.path.abspath(__file__)).parent,
                                               "res", "datasets")}


def get_path(identifier: str) -> str:
    """
    Get the requested path from the package config.
    :param identifier: Path-type identifier.
    :return:
    """
    return CONFIG["Paths"][identifier]


# def merge(source_path):
#     """
#     Merges several ElementTrees into one.
#
#     :return: Merged Elementtree
#     """
#     from lxml import etree as et
#     path = source_path
#     files = parseable_files(path)
#     base = et.parse(files[0])
#     root = base.getroot()
#     nsmap = root.nsmap
#     for file in files[1:]:
#         tree = et.parse(file)
#         for key, value in tree.getroot().nsmap.items():
#             if key in nsmap and value != nsmap[key]:
#                 log.error("Incompatible namespaces in schema files")
#             nsmap[key] = value
#         for child in tree.getroot():
#             root.append(child)
#     tree = et.ElementTree(root)
#     et.cleanup_namespaces(tree, top_nsmap=nsmap, keep_ns_prefixes=nsmap.keys())
#     return tree


def parseable_files(path):
    """
    Identify the parseable files within a directory (.xml/.rdf)
    :param path: path to the directory
    :return: list of files
    """
    if not isinstance(path, Path) and (path.endswith(".rdf") or path.endswith(".xml")):
        files = [path]
    elif not isinstance(path, Path) and path.endswith(".zip"):
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


def apply_xpath(expr, descriptions):
    by_profile = {profile: expr(values) for profile, values in descriptions.items()}
    return list(chain(*(v for v in by_profile.values()))), by_profile


def merge_results(results):
    if len(set(results)) == 1:
        return next(iter(results))
    elif not results:
        return None
    else:
        raise ValueError


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
    if not names:
        return None
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
