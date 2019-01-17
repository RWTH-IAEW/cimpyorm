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

import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from cimpyorm import common, get_path, log
import cimpyorm.Backend.auxiliary as aux


def configure(schemata=None, datasets=None):
    config = configparser.ConfigParser()
    config.read(get_path("CONFIGPATH"))
    if schemata:
        config["Paths"]["SCHEMAROOT"] = os.path.abspath(schemata)
    if datasets:
        config["Paths"]["DATASETROOT"] = os.path.abspath(datasets)
    with open(get_path("CONFIGPATH"), 'w') as configfile:
        config.write(configfile)


def load(path, schema=None):
    """
    Load a database from a .db file and make it queryable
    :param path: path to the database (str or os.path)
    :param schema:
    :return: (db_session, model namespace)
    """
    if not os.path.isfile(path):
        raise FileNotFoundError
    import cimpyorm.Backend.Instance as Instance
    import cimpyorm.Backend.Source as Source
    engine = sa.create_engine(f"sqlite:///{path}")
    Session = scoped_session(sessionmaker(bind=engine))
    session = Session()

    reset(engine)

    if not schema:
        si = session.query(Source.SourceInfo).first()
        v = si.cim_version
        log.info(f"CIM Version {v}")
        schema = Instance.Schema(session)
    else:
        pass
    schema.init_model(session)
    model = schema.model
    return session, model


def parse(dataset=None, db_name=":inplace:", echo=False):
    """
    Parse a database into a .db file and make it queryable
    :param dataset: path to the cim model (str or os.path)
    :param db_name: database name, defaults to "out.db" in source
    :param echo: Echo parameter for sqlalchemy engine
    :return: (db_session, model namespace)
    """
    import cimpyorm.Backend.Source as Source
    from cimpyorm import Parser
    db_path = Parser.prepare_path(dataset, db_name)
    engine, session = Parser.bind_db(echo, db_path)
    reset(engine)

    sources = Parser.get_sources(session, dataset, Source.SourceInfo)

    cim_version = Parser.get_cim_version(sources)
    rdfs_path = aux.find_rdfs_path(cim_version)
    nsmap = Parser.get_nsmap(sources)

    schema = common.generate_schema(rdfs_path=rdfs_path, session=session)
    Parser.init_backend(engine, nsmap, schema)

    log.info(f"Parsing data.")
    if engine.dialect.name == "mysql":
        log.debug("Deferring foreign key checks in mysql database.")
        session.execute("set foreign_key_checks=0")
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


def reset(engine):
    """
    Reset the table metadata for declarative classes.
    :param engine: A sqlalchemy db-engine
    :return: None
    """
    import cimpyorm.Backend.Elements as Elements
    import cimpyorm.Backend.Instance as Instance
    import cimpyorm.Backend.Source as Source
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
    db_session, m = parse([os.path.abspath(os.path.join(root, folder)) for folder in os.listdir(root) if
                           os.path.isdir(os.path.join(root, folder)) or
                           os.path.join(root, folder).endswith(".zip")])
    #root = get_path("DATASETROOT")
    #db_session, m = parse(os.path.join(get_path("DATASETROOT"), "FullGrid"))
    print(db_session.query(m.IdentifiedObject).first().name)  # pylint: disable=no-member
    db_session.close()
