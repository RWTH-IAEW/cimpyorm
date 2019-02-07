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
from zipfile import ZipFile

from lxml import etree as et
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cimpyorm import log
from cimpyorm.Model.Instance import Schema


def generate_schema(rdfs_path=None, session=None):
    """
    Generate the model from cim schema files in rdfs_path
    :param rdfs_path: str or os.path to schema file
    :param session: (optional) sqlalchemy session
    :return: classes and their inheritance hierarchy
    """
    from cimpyorm.Model.Instance import SchemaInfo
    if not rdfs_path:
        raise FileNotFoundError("Failed to find schema file. Please provide one.")
    tree = merge(rdfs_path)
    root = tree.getroot()
    if not session:
        engine = create_engine('sqlite:///:memory:', echo=False)
        Session = sessionmaker(bind=engine)
        session = Session()
    log.info(f"Dynamic code generation.")
    schema = Schema(session, merge(rdfs_path))
    session.add(SchemaInfo(root.nsmap))
    schema.init_model(session)
    return schema


def merge(source_path):
    """
    Merges several ElementTrees into one.

    :return: Merged Elementtree
    """
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
