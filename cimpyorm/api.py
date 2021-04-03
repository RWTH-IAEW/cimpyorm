#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

import os
from pathlib import Path
import configparser
from typing import Union, Tuple
from argparse import Namespace
from io import BytesIO
from zipfile import ZipFile

from sqlalchemy.orm.session import Session
from pandas import DataFrame, pivot_table
from tqdm import tqdm
from defusedxml.lxml import tostring
from lxml.etree import _ElementTree # nosec: Used for typechecking

from cimpyorm.auxiliary import get_logger, get_path, find_rdfs_path
from cimpyorm.Model.Schema import Schema, CIMClass, CIMEnum, CIMEnumValue
from cimpyorm.backends import SQLite, Engine
from cimpyorm.Writer import SingleFileSerializer, MultiFileSerializer
from cimpyorm.Model.Elements.Base import CIMProfile

log = get_logger(__name__)


def configure(schemata: Union[Path, str] = None,
              datasets: Union[Path, str] = None):
    """
    Configure paths to schemata or update the DATASETROOT used for tests.

    :param schemata: Path to a folder containing CIM schema descriptions.

    :param datasets: Path to a folder containing test datasets.
    """
    raise DeprecationWarning("Configuration of schema resources has been deprecated. Please get in "
                             "touch if you rely on this functionality.")
    config = configparser.ConfigParser()
    config.read(get_path("CONFIGPATH"))
    if schemata:
        config["Paths"]["SCHEMAROOT"] = os.path.abspath(schemata)
    if datasets:
        config["Paths"]["DATASETROOT"] = os.path.abspath(datasets)
    with open(get_path("CONFIGPATH"), 'w') as configfile:
        config.write(configfile)


def load(path_to_db: Union[Engine, str],
         echo: bool = False) -> Tuple[Session, Namespace]:
    """
    Load an already parsed database from disk or connect to a server and yield a database session to start querying on
    with the classes defined in the model namespace_name.

    Afterwards, the database can be queried using SQLAlchemy query syntax, providing the CIM classes contained in the
    :class:`~argparse.Namespace` return value.

    :param path_to_db: Path to the cim snapshot or a :class:`~cimpyorm.backend.Engine`.
    :param echo: Echo the SQL sent to the backend engine (SQLAlchemy option).

    :return: :class:`sqlalchemy.orm.session.Session`, :class:`argparse.Namespace`
    """
    import cimpyorm.Model.Schema as Schema
    from cimpyorm.Model import Source
    if isinstance(path_to_db, Engine):
        _backend = path_to_db
        _backend.echo = _backend.echo or echo
    elif os.path.isfile(path_to_db):
        _backend = SQLite(path_to_db, echo)
    else:
        raise FileNotFoundError(f"Unable to connect to database {path_to_db}")

    session = _backend.ORM
    _backend.reset()

    _si = session.query(Source.SourceInfo).first()
    try:
        v = _si.cim_version
    except AttributeError:
        v = 16
        log.warning(f"No CIM-version information found in dataset. Defaulting to: CIMv{v}")
    log.info(f"CIM Version {v}")
    schema = Schema.Schema(session)
    schema._generate_ORM(session)
    model = schema.model
    return session, model


def parse(dataset: Union[str, Path],
          backend=SQLite,
          schema: Union[str, Path] = None,
          log_to_file: Union[bool, Path, str] = False,
          silence_tqdm: bool = False) -> Tuple[Session, Namespace]:
    """
    Parse a database into a database backend and yield a database session to start querying on with the classes defined
    in the model namespace_name.

    Afterwards, the database can be queried using SQLAlchemy query syntax, providing the CIM classes contained in the
    :class:`~argparse.Namespace` return value.

    :param dataset: Path to the cim snapshot.
    :param backend: Database backend to be used (defaults to a SQLite on-disk database in the dataset location).
    :param schema: Location of the RDF schema to be used to parse the dataset (Folder of multiple RDF schemata or a
        single schema file).
    :param log_to_file: Pass logging output to a file for this ingest only.
    :param silence_tqdm: Silence tqdm progress bars

    :return: :class:`sqlalchemy.orm.session.Session`, :class:`argparse.Namespace`
    """
    #   Imports in function are due to SQLAlchemy table initialisation
    from cimpyorm import Parser
    if log_to_file:
        handler, packagelogger = create_logfile(dataset, log_to_file)
    try:
        backend = backend()
    except TypeError:
        pass
    backend.update_path(dataset)
    #   Reset database
    backend.drop()
    backend.reset()
    #   And connect
    engine, session = backend.connect()

    files = Parser.get_files(dataset)
    from cimpyorm.Model.Source import SourceInfo
    sources = frozenset([SourceInfo(file) for file in files])
    session.add_all(sources)
    session.commit()
    if not schema:
        #   Try to infer the CIM schema
        cim_version = Parser.get_cim_version(sources)
        rdfs_path = find_rdfs_path(cim_version)
    else:
        rdfs_path = schema
    model_schema = Schema(dataset=session, rdfs_path=rdfs_path)
    backend.generate_tables(model_schema)

    log.info(f"Parsing data.")
    entries = Parser.merge_sources(sources, model_schema)
    elements = Parser.parse_entries(entries, model_schema, silence_tqdm=silence_tqdm)
    log.info(f"Passing {len(elements):,} objects to database.")
    session.bulk_save_objects(elements)
    session.flush()
    log.debug(f"Start commit.")
    session.commit()
    log.debug(f"Finished commit.")

    if engine.dialect.name == "mysql":
        log.debug("Enabling foreign key checks in mysql database.")
        session.execute("SET foreign_key_checks='ON'")

    log.info("Finished.")

    model = model_schema.model
    if log_to_file:
        packagelogger.removeHandler(handler)
    return session, model


def create_empty_dataset(version="16",
                         backend=SQLite,
                         profile_whitelist=None):
    """
    Create an empty dataset to be filled by instantiation of schema-objects.

    :param version: The CIM version to be used.
    :param backend: The backend to be used.
    :param profile_whitelist: A list of allowed CIM-profiles. Profiles not contained in this list
        are not considered for schema generation. If empty or None, all schema elements defined by
        the CIM schema are generated.

    :return: An empty dataset/session object and the model namespace associated with the CIM schema.
    """
    try:
        backend = backend()
        backend.drop()
        backend.reset()
    except TypeError:
        pass
    dataset = backend.ORM
    rdfs_path = find_rdfs_path(version)
    schema = Schema(dataset=dataset, rdfs_path=rdfs_path, profile_whitelist=profile_whitelist)
    backend.generate_tables(schema)
    return dataset, schema.model


def create_logfile(dataset, log_to_file):
    from cimpyorm import log as packagelogger
    from cimpyorm.auxiliary import get_file_handler
    if not isinstance(log_to_file, bool) and os.path.isabs(log_to_file):
        handler = get_file_handler(log_to_file)
    elif os.path.isfile(dataset):
        if isinstance(log_to_file, (str, Path)):
            logfile = os.path.join(os.path.dirname(dataset), log_to_file)
        else:
            logfile = os.path.join(os.path.dirname(dataset), "import.log")
        handler = get_file_handler(logfile)
    else:
        handler = get_file_handler(os.path.join(dataset, "import.log"))
    packagelogger.addHandler(handler)
    return handler, packagelogger


def stats(session):
    from cimpyorm.Model.Elements import CIMClass
    from collections import Counter
    stats = {}
    objects = Counter()
    for base_class in session.query(CIMClass).filter(CIMClass.parent==None).all():
        objects |= Counter([el.type_ for el in session.query(base_class.class_).all()])
    for cimclass in session.query(CIMClass).all():
        cnt = session.query(cimclass.class_).count()
        if cnt > 0:
            if cimclass.name in objects:
                stats[cimclass.name] = (cnt, objects[cimclass.name])
            else:
                stats[cimclass.name] = (cnt, 0)
    return DataFrame(stats.values(), columns=["polymorphic_instances", "objects"],
                     index=stats.keys()).sort_values("objects", ascending=False)


def lint(session, model):
    """
    Check the model for missing obligatory values and references and for invalid references (foreign key validation)
    and return the results in a pandas pivot-table.

    :param session: The SQLAlchemy session object (obtained from parse/load).
    :param model: The parsed CIMPyORM model (obtained from parse/load).

    :return: Pandas pivot-table.
    """
    events = []
    for CIM_class in tqdm(model.schema.class_hierarchy("dfs"), desc=f"Linting...", leave=True):
        query = session.query(CIM_class.class_)
        for prop in CIM_class.props:
            if not prop.optional and prop.used:
                total = query.count()
                objects = query.filter_by(**{prop.name: None}).count()
                if objects:
                    events.append({"Class": CIM_class.name,
                                   "Property": prop.full_name,
                                   "Total": total,
                                   "Type": "Missing",
                                   "Violations": objects,
                                   "Unique": None})
                    log.debug(f"Missing mandatory property {prop.full_name} for "
                              f"{objects} instances of type {CIM_class.name}.")
                if prop.range:
                    try:
                        if isinstance(prop.range, CIMClass):
                            col = getattr(CIM_class.class_, prop.full_name+"_id")
                            validity = session.query(col).except_(session.query(
                                prop.range.class_.id))
                        elif isinstance(prop.range, CIMEnum):
                            col = getattr(CIM_class.class_, prop.full_name + "_get_name")
                            validity = session.query(col).except_(session.query(CIMEnumValue.name))
                    except AttributeError:
                        log.warning(f"Couldn't determine validity of {prop.full_name} on "
                                    f"{CIM_class.name}. The linter does not yet support "
                                    f"many-to-many relationships.")
                        # ToDo: Association table errors are currently not caught
                    else:
                        count = validity.count()
                        # query.except() returns (None) if right hand side table is empty
                        if count > 1 or (count == 1 and tuple(validity.one())[0] is not None):
                            non_unique = query.filter(col.in_(
                                val[0] for val in validity.all())).count()
                            events.append({"Class": CIM_class.name,
                                           "Property": prop.full_name,
                                           "Total": total,
                                           "Type": "Invalid",
                                           "Violations": non_unique,
                                           "Unique": count
                                           })

    return pivot_table(DataFrame(events), values=["Violations", "Unique"],
                       index=["Type", "Class", "Total", "Property"])


def docker_parse() -> None:
    """
    Dummy function for parsing in shared docker tmp directory.
    """
    raise DeprecationWarning("Par. Please get in "
                             "touch if you rely on this functionality.")
    # Fixme: Reported by bandit as hardcoded_tmp_directory
    # parse(r"/tmp")


def describe(element, fmt: str = "psql") -> None:
    """
    Give a description of an object.

    :param element: The element to describe.

    :param fmt: Format string for tabulate package (default postgres formatting).
    """
    try:
        element.describe(fmt)
    except AttributeError:
        print(f"Element of type {type(element)} doesn't provide descriptions.")


def serialize(dataset, mode="Single", profile_whitelist=None, header_data=None):
    """
    Serialize the dataset according to the options given by the parameters and the data loaded in
        the dataset.

    :param dataset: The dataset/SQLAlchemy-Session object to export.
    :param mode: The export Mode, e.g. Single-File or Split by profiles.
    :param profile_whitelist: The profiles to export. Mandatory if Export-mode is 'Multi'
    :param header_data: Additional information for creating the file-headers (FullModel Objects).
        This is a dictionary of CIM-header supplements. Currently only the 'profile_header'
        field is supported, in which a list of profile identifiers (e.g.
        'http://entsoe.eu/CIM/Topology/4/1') are defined.

    :return: A serialization of the model (or a list thereof if mode='Multi')
    """
    if isinstance(profile_whitelist, str):
        profile_whitelist = (profile_whitelist,)
    if not profile_whitelist and not mode == "Single":
        raise ValueError("Incompatible modes.")
    if mode == "Single":
        serializer = SingleFileSerializer(dataset)
    elif mode == "Multi":
        serializer = MultiFileSerializer(dataset)
    else:
        raise ValueError(f"Unknown serializer mode: {mode}")
    return serializer.build_tree(profile_whitelist, header_data=header_data)


def export(dataset, mode: str = "Single",
           profile_whitelist: Union[list, tuple] = None,
           header_data: dict = None):
    """
    Returns a XML-serialization of the dataset within a zip-Archive.

    :param dataset: The dataset/SQLAlchemy-Session object to export.
    :param mode: {'Single', 'Multi'} - The export Mode, e.g. Single-File or Split by profiles.
    :param profile_whitelist: The profiles to export. Mandatory if Export-mode is 'Multi'.
        Profiles can be specificed by their full name (e.g. 'EquipmentProfile' or their short name 'EQ')
    :param header_data: Additional information for creating the file-headers (FullModel Objects).
        This is a dictionary of CIM-header supplements. Currently only the 'profile_header'
        field is supported, in which a list of profile identifiers (e.g.
        'http://entsoe.eu/CIM/Topology/4/1') are defined.

    :return: A BytesIO-filehandle containing a zip-archive of the export.
    """
    if not mode in ("Multi", "Single"):
        raise ValueError("Unknown serialization mode ('Single' and 'Multi' are valid values)")
    trees = serialize(dataset, mode, profile_whitelist, header_data)
    if isinstance(trees, _ElementTree):
        trees = [trees]
    file = BytesIO()

    with ZipFile(file, "w") as zf:
        if not profile_whitelist or mode == "Single":
            if not len(trees) == 1:
                raise ValueError("Too many objects returned by serializer.")
            zf.writestr(
                f"Export.xml",
                tostring(trees[0],
                         encoding="UTF-8",
                         xml_declaration=True,
                         pretty_print=True)
            )
        else:
            profile_lib = {p.name: p for p in dataset.query(CIMProfile)}
            for profile, tree in zip(profile_whitelist, trees):
                fname = profile_lib[profile].short
                zf.writestr(
                    f"{fname}.xml",
                    tostring(tree,
                             encoding="UTF-8",
                             xml_declaration=True,
                             pretty_print=True)
                )
    return file
