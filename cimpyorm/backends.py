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
from importlib import reload
from abc import ABC

import sqlalchemy as sa
from sqlalchemy.engine import Engine as SA_Engine
from sqlalchemy.exc import OperationalError, InternalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from networkx import bfs_tree

# pylint: disable=too-many-arguments
import cimpyorm.Model.Elements.Datatype
import cimpyorm.Model.auxiliary as aux
from cimpyorm.auxiliary import get_logger, Dataset

log = get_logger(__name__)


class Engine(ABC):
    def __init__(self, dialect=None, echo=False, driver=None, path=None):
        self.dialect = dialect
        self.echo = echo
        self.driver = driver
        self.path = path
        self._engine = None

    @property
    def engine(self) -> SA_Engine:
        if not self._engine:
            log.info(f"Database: {self}.")
            engine = self._connect_engine()
            self._engine = engine
        return self._engine

    @property
    def ORM(self) -> Dataset:
        Session = sessionmaker(bind=self.engine, class_=Dataset)
        ORM = Session()
        return ORM

    def connect(self):
        self.configure()
        return self.engine, self.ORM

    def configure(self):
        pass

    def update_path(self, path):
        pass

    def _prefix(self):
        if self.driver:
            return f"{self.dialect}+{self.driver}"
        else:
            return f"{self.dialect}"

    def _connect_engine(self):
        raise NotImplementedError

    def drop(self):
        raise NotImplementedError

    def reset(self) -> None:
        """
        Reset the table metadata for declarative classes.
        """
        import cimpyorm.Model.Elements as Elements
        import cimpyorm.Model.Schema as Schema
        import cimpyorm.Model.Source as Source
        aux.Base = declarative_base(self.engine)
        reload(Source)
        reload(Elements)
        reload(Schema)
        # aux.Base.metadata.create_all(self.engine)
        self.configure()

        Source.SourceInfo.metadata.create_all(self.engine)
        Elements.CIMProfile.metadata.create_all(self.engine)
        Elements.CIMNamespace.metadata.create_all(self.engine)
        Elements.CIMClass.metadata.create_all(self.engine)
        Elements.CIMProp.metadata.create_all(self.engine)
        Elements.CIMPackage.metadata.create_all(self.engine)
        Elements.CIMEnum.metadata.create_all(self.engine)
        Elements.CIMEnumValue.metadata.create_all(self.engine)
        cimpyorm.Model.Elements.Datatype.CIMDT.metadata.create_all(self.engine)
        Schema.SchemaInfo.metadata.create_all(self.engine)

    def generate_tables(self, schema):
        g, classes = schema.get_inheritance_graph()
        hierarchy = list(bfs_tree(g, "__root__"))
        hierarchy.remove("__root__")
        log.info(f"Creating map prefixes.")
        # ToDo: create_all is quite slow, maybe this can be sped up. Currently low priority.
        log.info(f"Creating table metadata.")
        for child_identifier in g["__root__"]:
            classes[child_identifier].class_.metadata.create_all(self.engine)
        log.info(f"Backend model ready.")


class SQLite(Engine):
    def __init__(self, path="out.db", echo=False, driver=None, dataset_loc=None):
        """
        Default constructor for SQLite backend instance

        :param path: Storage location for the .db-file (default: "out.db" in cwd)

        :param echo: SQLAlchemy "echo" parameter (default: False)

        :param driver: Python SQLite driver (default: sqlite3)

        :param dataset_loc: Dataset location used to automatically determine storage location (in the dataset folder)
        """
        self.dialect = "sqlite"
        super().__init__(self.dialect, echo, driver, path)

    def __str__(self):
        return f"{self.__class__.__name__} at {self.path}"

    def drop(self):
        try:
            os.remove(self.path)
            log.info(f"Removed old database {self.path}.")
            self._engine = None
        except FileNotFoundError:
            pass
        except PermissionError:
            raise UserWarning("Can't remove database while there is still a connection open. "
                              "Please close the connection first using session.close().")

    @property
    def engine(self):
        return super().engine

    def update_path(self, path):
        if path is None:
            out_dir = os.getcwd()
        elif isinstance(path, list):
            try:
                out_dir = os.path.commonpath([os.path.abspath(path) for path in path])
            except ValueError:
                # Paths are on different drives - default to cwd.
                log.warning(f"Datasources have no common root. Database file will be saved to {os.getcwd()}")
                out_dir = os.getcwd()
        else:
            out_dir = os.path.abspath(path)
        if not os.path.isabs(self.path):
            if os.path.isdir(out_dir):
                db_path = os.path.join(out_dir, self.path)
            else:
                db_path = os.path.join(os.path.dirname(out_dir), "out.db")
        else:
            db_path = os.path.abspath(self.path)
        self.path = db_path

    def _connect_engine(self):
        # ToDo: Disabling same_thread check is only treating the symptoms, however, without it, property changes
        #       can't be committed
        return sa.create_engine(f"{self._prefix()}:///{self.path}",
                                echo=self.echo, connect_args={"check_same_thread": False})


class InMemory(Engine):
    def __init__(self, echo=False, driver=None):
        """
        Default constructor for In-Memory-SQLite instances

        :param echo: SQLAlchemy "echo" parameter (default: False)

        :param driver: Python SQLite driver (default: sqlite3)
        """
        self.dialect = "sqlite"
        super().__init__(self.dialect, echo, driver)

    def __str__(self):
        return f"{self.__class__.__name__}"

    def drop(self):
        pass

    def _connect_engine(self):
        # ToDo: Disabling same_thread check is only treating the symptoms, however, without it, property changes
        #       can't be committed
        return sa.create_engine(f"{self._prefix()}:///:memory:",
                                echo=self.echo, connect_args={"check_same_thread": False})


class ClientServer(Engine):
    """
    A hosted backend (MariaDB, MySQL, Postgres) driver
    """
    def __init__(self, username=None, password=None, driver=None,
                 host=None, port=None, path=None, echo=False):
        super().__init__(None, echo, driver, path)
        self.username = username
        self.password = password
        self.hostname = host
        self.port = port

    def __str__(self):
        return f"{self.__class__.__name__} at {self.remote_path}"

    @property
    def remote_path(self):
        if self.path:
            return f"{self.host}/{self.path}"
        else:
            return self.host

    def configure(self):
        self.engine.execute(f"ALTER DATABASE {self.path} DEFAULT CHARACTER SET utf8 COLLATE `utf8_bin`;")

    @property
    def host(self):
        return f"{self.hostname}:{self.port}"

    def drop(self):
        try:
            log.info(f"Dropping database {self.path} at {self.host}.")
            self.engine.execute(f"DROP DATABASE {self.path};")
        except OperationalError:
            pass
        self._engine = None

    def _credentials(self):
        return f"{self.username}:{self.password}"

    def _connect_engine(self):
        engine = sa.create_engine(
            f"{self._prefix()}://{self._credentials()}@{self.remote_path}", echo=self.echo)
        try:
            engine.connect()
            # Pymysql error is raised as InternalError
        except (OperationalError, InternalError):
            engine = sa.create_engine(
                f"{self._prefix()}://{self._credentials()}@{self.host}", echo=self.echo)
            engine.execute(f"CREATE SCHEMA {self.path};")
            engine = sa.create_engine(
                f"{self._prefix()}://{self._credentials()}@{self.remote_path}", echo=self.echo)
        return engine


class MariaDB(ClientServer):
    def __init__(self, username="root", password="", driver="pymysql",  # nosec: This is just a client connector with
                 # an empty default password, so this is a false positive. This package does not (except for its test
                 # databases) start any database servers related to these values.
                 host="127.0.0.1", port=3306, path="cim", echo=False):
        """
        Default constructor for MariaDB backend instance

        :param username: Username for the MariaDB database (default: root)

        :param password: Password for username (at) MariaDB database (default: "")

        :param driver: Python MariaDB driver (default: mysqlclient)

        :param host: Database host (default: localhost)

        :param port: Database port (default: 3306)

        :param path: Database name (default: "cim")

        :param echo: SQLAlchemy "echo" parameter (default: False)
        """
        super().__init__(username, password, driver, host,
                         port, path, echo)
        self.dialect = "mysql"

    @property
    def ORM(self):
        session = super().ORM
        log.debug("Deferring foreign key checks in mysql database.")
        session.execute("SET foreign_key_checks='OFF'")
        return session


class MySQL(ClientServer):
    def __init__(self, username="root", password="", driver="pymysql",  # nosec: This is just a client connector with
                 # an empty default password, so this is a false positive. This package does not (except for its test
                 # databases) start any database servers related to these values.
                 host="127.0.0.1", port=3306, path="cim", echo=False):
        """
        Default constructor for MySQL backend instance

        :param username: Username for the MySQL database (default: root)

        :param password: Password for username (at) MySQL database (default: "")

        :param driver: Python MariaDB driver (default: pymysql)

        :param host: Database host (default: localhost)

        :param port: Database port (default: 3306)

        :param path: Database name (default: "cim")

        :param echo: SQLAlchemy "echo" parameter (default: False)
        """
        super().__init__(username, password, driver, host,
                         port, path, echo)
        self.dialect = "mysql"

    @property
    def ORM(self):
        session = super().ORM
        log.debug("Deferring foreign key checks in mysql database.")
        session.execute("SET foreign_key_checks='OFF'")
        return session
