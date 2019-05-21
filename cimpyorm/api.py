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
from pathlib import Path
import configparser
from typing import Union, Tuple
from argparse import Namespace

from sqlalchemy.orm.session import Session
from pandas import DataFrame, pivot_table
from tqdm import tqdm

from cimpyorm.auxiliary import log, get_path
from cimpyorm.Model.Schema import Schema, CIMClass, CIMEnum, CIMEnumValue
from cimpyorm.backends import SQLite, Engine, InMemory


def configure(schemata: Union[Path, str] = None, datasets: Union[Path, str] = None):
    """
    Configure paths to schemata or update the DATASETROOT used for tests.

    :param schemata: Path to a folder containing CIM schema descriptions.

    :param datasets: Path to a folder containing test datasets.
    """
    config = configparser.ConfigParser()
    config.read(get_path("CONFIGPATH"))
    if schemata:
        config["Paths"]["SCHEMAROOT"] = os.path.abspath(schemata)
    if datasets:
        config["Paths"]["DATASETROOT"] = os.path.abspath(datasets)
    with open(get_path("CONFIGPATH"), 'w') as configfile:
        config.write(configfile)


def load(path_to_db: Union[Engine, str], echo: bool = False) -> Tuple[Session, Namespace]:
    """
    Load an already parsed database from disk or connect to a server and yield a database session to start querying on
    with the classes defined in the model namespace.

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
        raise NotImplementedError(f"Unable to connect to database {path_to_db}")

    session = _backend.session
    _backend.reset()

    _si = session.query(Source.SourceInfo).first()
    v = _si.cim_version
    log.info(f"CIM Version {v}")
    schema = Schema.Schema(session)
    schema.init_model(session)
    model = schema.model
    return session, model


def parse(dataset: Union[str, Path],
          backend: Engine = SQLite(),
          silence_tqdm: bool = False) -> Tuple[Session, Namespace]:
    """
    Parse a database into a database backend and yield a database session to start querying on with the classes defined
    in the model namespace.

    Afterwards, the database can be queried using SQLAlchemy query syntax, providing the CIM classes contained in the
    :class:`~argparse.Namespace` return value.

    :param dataset: Path to the cim snapshot.
    :param backend: Database backend to be used (defaults to a SQLite on-disk database in the dataset location).
    :param silence_tqdm: Silence tqdm progress bars

    :return: :class:`sqlalchemy.orm.session.Session`, :class:`argparse.Namespace`
    """
    from cimpyorm import Parser
    backend.update_path(dataset)
    # Reset database
    backend.drop()
    backend.reset()
    # And connect
    engine, session = backend.connect()

    files = Parser.get_files(dataset)
    from cimpyorm.Model.Source import SourceInfo
    sources = frozenset([SourceInfo(file) for file in files])
    session.add_all(sources)
    session.commit()

    cim_version = Parser.get_cim_version(sources)

    schema = Schema(version=cim_version, session=session)
    backend.generate_tables(schema)

    log.info(f"Parsing data.")
    entries = Parser.merge_sources(sources)
    elements = Parser.parse_entries(entries, schema, silence_tqdm=silence_tqdm)
    log.info(f"Passing {len(elements):,} objects to database.")
    session.bulk_save_objects(elements)
    session.flush()
    log.debug(f"Start commit.")
    session.commit()
    log.debug(f"Finished commit.")

    if engine.dialect.name == "mysql":
        log.debug("Enabling foreign key checks in mysql database.")
        session.execute("SET foreign_key_checks='ON'")

    log.info("Exit.")

    model = schema.model
    return session, model


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
    events = []
    for CIM_class in tqdm(model.schema.class_hierarchy("dfs"), desc=f"Linting...", leave=True):
        query = session.query(CIM_class.class_)
        for prop in CIM_class.props:
            if not prop.optional and prop.used:
                total = query.count()
                objects = query.filter_by(**{prop.full_label: None}).count()
                if objects:
                    events.append({"Class": CIM_class.label,
                                   "Property": prop.full_label,
                                   "Total": total,
                                   "Type": "Missing",
                                   "Violations": objects,
                                   "Unique": None})
                    log.debug(f"Missing mandatory property {prop.full_label} for "
                              f"{objects} instances of type {CIM_class.label}.")
                if prop.range:
                    try:
                        if isinstance(prop.range, CIMClass):
                            col = getattr(CIM_class.class_, prop.full_label+"_id")
                            validity = session.query(col).except_(session.query(
                                prop.range.class_.id))
                        elif isinstance(prop.range, CIMEnum):
                            col = getattr(CIM_class.class_, prop.full_label + "_name")
                            validity = session.query(col).except_(session.query(CIMEnumValue.name))
                    except AttributeError:
                        log.warning(f"Couldn't determine validity of {prop.full_label} on "
                                    f"{CIM_class.label}. The linter does not yet support "
                                    f"many-to-many relationships.")
                        # ToDo: Association table errors are currently not caught
                    else:
                        count = validity.count()
                        # query.except() returns (None) if right hand side table is empty
                        if count > 1 or (count == 1 and tuple(validity.one())[0] is not None):
                            non_unique = query.filter(col.in_(
                                val[0] for val in validity.all())).count()
                            events.append({"Class": CIM_class.label,
                                           "Property": prop.full_label,
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
    parse(r"/tmp")


def describe(element, fmt: str = "psql") -> None:
    """
    Give a description of an object.

    :param element: The element to describe.

    :param fmt: Format string for tabulate package.
    """
    try:
        element.describe(fmt)
    except AttributeError:
        print(f"Element of type {type(element)} doesn't provide descriptions.")


if __name__ == "__main__":
    root = get_path("DATASETROOT")
    session, model = load(os.path.join(root, "FullGrid", "out.db"))
    print(lint(session, model))
    # # db_session, m = parse([os.path.abspath(os.path.join(root, folder)) for folder in os.listdir(root) if
    # #                        os.path.isdir(os.path.join(root, folder)) or
    # #                        os.path.join(root, folder).endswith(".zip")])
    # db_session, m = parse(os.path.join(get_path("DATASETROOT"), "FullGrid"), InMemory())
    # print(db_session.query(m.IdentifiedObject).first().name)  # pylint: disable=no-member
    # db_session.close()
