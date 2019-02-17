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
from collections import defaultdict
from functools import lru_cache

from networkx import bfs_tree

from cimpyorm import log
from cimpyorm.common import parseable_files
from cimpyorm.Model.auxiliary import HDict


def prepare_path(dataset, db_name=None):
    """
    Delete old databases when re-parsing
    :param dataset: path to cim files
    :param db_name: database name, defaults to "out.db" in source
    :return: Path for the database file
    """
    if dataset is None:
        out_dir = os.getcwd()
    elif isinstance(dataset, list):
        try:
            out_dir = os.path.commonpath([os.path.abspath(path) for path in dataset])
        except ValueError:
            # Paths are on different drives - default to cwd.
            log.warning(f"Datasources have no common root. Database file will be saved to {os.getcwd()}")
            out_dir = os.getcwd()
    else:
        out_dir = os.path.abspath(dataset)
    if db_name == ":inplace:":
        if os.path.isdir(out_dir):
            db_path = os.path.join(out_dir, "out.db")
        else:
            db_path = os.path.join(os.path.dirname(out_dir), "out.db")
    else:
        db_path = os.path.abspath(db_name)
    try:
        os.remove(db_path)
        log.info(f"Removed old database {db_path}.")
    except FileNotFoundError:
        pass
    return db_path


def get_sources(db_session, dataset, SourceInfo):
    if isinstance(dataset, list):
        files = chain(*[parseable_files(path) for path in dataset])
    else:
        files = parseable_files(dataset)
    source_id = 1
    sources = []
    for file in files:
        si = SourceInfo(file, source_id)
        db_session.add(si)
        source_id = source_id + 1
        sources.append(si)
    db_session.commit()
    return frozenset(sources)


def merge_sources(sources):
    d_ = defaultdict(dict)
    from lxml.etree import XPath
    xp = {"id": XPath("@rdf:ID", namespaces=get_nsmap(sources)),
          "about": XPath("@rdf:about", namespaces=get_nsmap(sources))}
    for source in sources:
        for element in source.tree.getroot():
            try:
                uuid = determine_uuid(element, xp)
                classname = shorten_namespace(element.tag, HDict(get_nsmap(sources)))
                if classname not in d_ or uuid not in d_[classname].keys():
                    d_[classname][uuid] = element
                else:
                    [d_[classname][uuid].append(sub) for sub in element]
            except ValueError:
                log.warning(f"Skipped element during merge: {element}.")
    return d_


def parse_entries(entries, schema):
    classes = dict(schema.session.query(
        schema._Element_classes["CIMClass"].name,
        schema._Element_classes["CIMClass"]
    ).all())
    created = []
    for classname, elements in entries.items():
        if classname in classes.keys():
            for uuid, element in elements.items():
                    created.append(classes[classname].class_(id="_"+uuid,
                                                             **classes[classname]._build_map(element, schema.session)))
        else:
            log.info(f"{classname} not implemented. Skipping.")
    return created


def determine_uuid(element, xp):
    uuid = None
    try:
        id = xp["id"](element)[0]
        if id.startswith("_"):
            id = id[1:]
        uuid = id
    except (IndexError):
        pass
    try:
        about = xp["about"](element)[0].split("urn:uuid:")[-1].split("#_")[-1]
        uuid = about
    except IndexError:
        pass
    return uuid


@lru_cache()
def get_nsmap(sources):
    """
    Return the merged namespace map for a list of data sources
    :param sources: list of DataSource objects
    :return: dict, merged nsmap of all DataSource objects
    """
    nsmaps = [source.nsmap for source in sources]
    nsmaps = {k: v for d in nsmaps for k, v in d.items()}
    return nsmaps


def get_cim_version(sources):
    """
    Return the (unambiguous) DataSource cim versions
    :param sources: DataSources
    :return:
    """
    cim_versions = [source.cim_version for source in sources]
    if len(set(cim_versions)) > 1:
        log.error(f"Ambiguous cim_versions: {cim_versions}.")
    return cim_versions[0]


def init_backend(engine, nsmap, schema):
    """
    :param engine: A Sqlalchemy DB backend engine (usually sqlite)
    :param nsmap: Namespace-Map of the CIM Model
    :param schema: The CIM Backend
    :return classes: The classes described in the schema file
    """
    g = schema.create_inheritance_graph()
    hierarchy = list(bfs_tree(g, "__root__"))
    hierarchy.remove("__root__")
    log.info(f"Creating map prefixes.")
    for c in hierarchy:
        c.class_.compile_map(nsmap)
    # ToDo: create_all is quite slow, maybe this can be sped up. Currently low priority.
    log.info(f"Creating table metadata.")
    for child in g["__root__"]:
        child.class_.metadata.create_all(engine)
    log.info(f"Backend model ready.")


def update_rows(elements, schema, db_session):
    """

    :param elements:
    :param m:
    :param db_session:
    :return:
    """
    for group in elements:
        if group:
            update = []
            refs = []
            for element in group:
                d = {key: value for key, value in vars(element).items() if key in
                     schema.Elements["CIMClass"][group[0].type_].prop_keys}
                d.update({"_about_ref": element._about_ref})
                update.append(d)
                refs.append(element._about_ref)
            objects = db_session.query(schema.Elements["CIMClass"][group[0].type_].class_).all()
            obj_dict = {}
            for obj in objects:
                obj_dict[obj.id] = obj
            for data in update:
                fields = dict.fromkeys(set(schema.Elements["CIMClass"][group[0].type_].prop_keys) &
                                       set(data.keys()))
                for key, _ in fields.items():
                    setattr(obj_dict[data["_about_ref"]], key, data.get(key))
            db_session.flush()
            log.debug(f"Updated {len(refs)} rows in table {group[0].type_}")

    log.debug(f"Added properties to {len(list(chain(*elements)))} elements")
    log.debug(f"Start commit.")
    db_session.commit()
    log.debug(f"Finished commit.")


@lru_cache()
def shorten_namespace(elements, nsmap):
    """
    Map a list of XML tag class names on the internal classes (e.g. with shortened namespaces)
    :param classes: list of XML tags
    :param nsmap: XML nsmap
    :return: List of mapped names
    """
    names = []
    islist = True
    if not (isinstance(elements, list) or isinstance(elements, frozenset)):
        elements = [elements]
        islist = False
    for el in elements:
        for key, value in nsmap.items():
            if value in el:
                if key == "cim":
                    names.append(el.split(value[-1]+"}")[-1])
                else:
                    names.append(el.replace("{"+value+"}", key+"_"))
        if el.startswith("#"):
            names.append(el.split("#")[-1])
    if not islist and len(names) == 1:
        names = names[0]
    return names
