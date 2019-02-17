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
from importlib import reload
import configparser
from typing import Union, Tuple
from argparse import Namespace

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session

from cimpyorm import common, get_path, log
import cimpyorm.Model.auxiliary as aux
from cimpyorm.backend import SQLite, Engine, InMemory
from cimpyorm.Model import Source


def configure(schemata=None, datasets=None):
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
    import cimpyorm.Model.Instance as Instance
    from cimpyorm.Model import Source
    if isinstance(path_to_db, Engine):
        _backend = path_to_db
        _backend.echo = _backend.echo or echo
    elif os.path.isfile(path_to_db):
        _backend = SQLite(path_to_db, echo)
    else:
        raise NotImplementedError(f"Unable to connect to database {path_to_db}")

    engine = _backend.engine
    session = _backend.session
    reset_model(engine)

    _si = session.query(Source.SourceInfo).first()
    v = _si.cim_version
    log.info(f"CIM Version {v}")
    schema = Instance.Schema(session)
    schema.init_model(session)
    model = schema.model
    return session, model


def parse(dataset: Union[str, Path], backend: Engine = SQLite()) -> Tuple[Session, Namespace]:
    """
    Parse a database into a database backend and yield a database session to start querying on with the classes defined
    in the model namespace.

    Afterwards, the database can be queried using SQLAlchemy query syntax, providing the CIM classes contained in the
    :class:`~argparse.Namespace` return value.

    :param dataset: Path to the cim snapshot.
    :param backend: Database backend to be used (defaults to a SQLite on-disk database in the dataset location).

    :return: :class:`sqlalchemy.orm.session.Session`, :class:`argparse.Namespace`
    """
    from cimpyorm import Parser
    backend.dataset_loc = dataset
    # Reset database
    backend.drop()
    # And connect
    engine = backend.engine
    reset_model(engine)
    session = backend.session

    # ToDo: Move to Engines
    if engine.dialect.name == "mysql":
        log.debug("Deferring foreign key checks in mysql database.")
        session.execute("SET foreign_key_checks='OFF'")
    elif engine.dialect.name == "postgresql":
        session.execute("SET CONSTRAINTS ALL DEFERRED")

    sources = Parser.get_sources(session, dataset, Source.SourceInfo)

    cim_version = Parser.get_cim_version(sources)
    rdfs_path = aux.find_rdfs_path(cim_version)
    nsmap = Parser.get_nsmap(sources)

    schema = common.generate_schema(rdfs_path=rdfs_path, session=session)
    Parser.init_backend(engine, nsmap, schema)

    log.info(f"Parsing data.")
    entries = Parser.merge_sources(sources)
    elements = Parser.parse_entries(entries, schema)
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


def reset_model(engine):
    """
    Reset the table metadata for declarative classes.
    :param engine: A sqlalchemy db-engine to reset
    :return: None
    """
    import cimpyorm.Model.Elements as Elements
    import cimpyorm.Model.Instance as Instance
    import cimpyorm.Model.Source as Source
    aux.Base = declarative_base(engine)
    reload(Source)
    reload(Elements)
    reload(Instance)
    Source.SourceInfo.metadata.create_all(engine)
    Instance.SchemaInfo.metadata.create_all(engine)


def docker_parse():
    """
    Dummy function for parsing in shared docker tmp directory
    :return: None
    """
    parse(r"/tmp")


if __name__ == "__main__":
    root = get_path("DATASETROOT")
    # db_session, m = parse([os.path.abspath(os.path.join(root, folder)) for folder in os.listdir(root) if
    #                        os.path.isdir(os.path.join(root, folder)) or
    #                        os.path.join(root, folder).endswith(".zip")])
    db_session, m = parse(os.path.join(get_path("DATASETROOT"), "FullGrid"), InMemory())
    print(db_session.query(m.IdentifiedObject).first().name)  # pylint: disable=no-member
    db_session.close()
