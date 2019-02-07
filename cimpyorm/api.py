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
from importlib import reload
import configparser

from sqlalchemy.ext.declarative import declarative_base

from cimpyorm import common, get_path, log
import cimpyorm.Model.auxiliary as aux
from cimpyorm.backend import SQLite, Engine
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


def load(db_path=None, echo=False):
    """
    Load a database from a .db file and make it queryable
    :param db_path: path to the database (str or os.path)
    :param schema:
    :param echo:
    :return: (db_session, model namespace)
    """
    import cimpyorm.Model.Instance as Instance
    from cimpyorm.Model import Source
    if isinstance(db_path, Engine):
        _backend = db_path
        _backend.echo = _backend.echo or echo
    elif os.path.isfile(db_path):
        _backend = SQLite(db_path, echo)
    else:
        raise NotImplementedError(f"Unable to connect to database {db_path}")

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


def parse(dataset, backend=SQLite()):
    """
    Parse a database into a .db file and make it queryable
    :param dataset: path to the cim model (str or os.path)

    :return: (db_session, model namespace)
    """
    from cimpyorm import Parser
    backend.dataset = dataset
    # Reset database
    backend.drop()
    # And connect
    engine = backend.engine
    reset_model(engine)
    session = backend.session

    # ToDo: Move to Engines
    if engine.dialect.name == "mysql":
        log.debug("Deferring foreign key checks in mysql database.")
        session.execute("set foreign_key_checks=0")
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
        session.execute("set foreign_key_checks=1")

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
    db_session, m = parse(os.path.join(get_path("DATASETROOT"), "FullGrid"))
    print(db_session.query(m.IdentifiedObject).first().name)  # pylint: disable=no-member
    db_session.close()
