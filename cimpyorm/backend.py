import os

import sqlalchemy as sa
from sqlalchemy.exc import OperationalError, InternalError
from sqlalchemy.orm import sessionmaker

from cimpyorm import log


class Engine:
    def __init__(self, dialect=None, echo=False, driver=None, path=None):
        self.dialect = dialect
        self.echo = echo
        self.driver = driver
        self.path = path
        self._engine = None

    @property
    def engine(self):
        """
        :param echo:
        :param database:
        :return:
        """
        if not self._engine:
            log.info(f"Database: {self.path}")
            engine = self._connect_engine()
            self._engine = engine
        return self._engine

    @property
    def session(self):
        session = sessionmaker(bind=self.engine)
        return session()

    def _prefix(self):
        if self.driver:
            return f"{self.dialect}+{self.driver}"
        else:
            return f"{self.dialect}"

    def _connect_engine(self):
        raise NotImplementedError

    def drop(self):
        raise NotImplementedError


class SQLite(Engine):
    def __init__(self, path="out.db", echo=False, driver=None, dataset_loc=None):
        self.dialect = "sqlite"
        self.dataset = dataset_loc
        super().__init__(self.dialect, echo, driver, path)
        self._update_path()

    def drop(self):
        try:
            os.remove(self.path)
            log.info(f"Removed old database {self.path}.")
            self._engine = None
        except FileNotFoundError:
            pass

    @property
    def engine(self):
        if not self._engine:
            self._update_path()
        return super().engine

    def _update_path(self):
        if self.dataset is None:
            out_dir = os.getcwd()
        elif isinstance(self.dataset, list):
            try:
                out_dir = os.path.commonpath([os.path.abspath(path) for path in self.dataset])
            except ValueError:
                # Paths are on different drives - default to cwd.
                log.warning(f"Datasources have no common root. Database file will be saved to {os.getcwd()}")
                out_dir = os.getcwd()
        else:
            out_dir = os.path.abspath(self.dataset)
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


class InMemory(SQLite):
    def _connect_engine(self):
        # ToDo: Disabling same_thread check is only treating the symptoms, however, without it, property changes
        #       can't be committed
        return sa.create_engine(f"{self._prefix()}:///:memory:",
                                echo=self.echo, connect_args={"check_same_thread": False})


class ClientServer(Engine):
    def __init__(self, username=None, password=None, driver=None,
                 host=None, port=None, path=None, echo=False):
        super().__init__(None, echo, driver, path)
        self.username = username
        self.password = password
        self.hostname = host
        self.port = port

    @property
    def remote_path(self):
        if self.path:
            return f"{self.host}/{self.path}"
        else:
            return self.host

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
            engine.execute(f"CREATE SCHEMA {self.path} DEFAULT CHARACTER SET utf8 COLLATE "
                           f"utf8_bin;")
            engine = sa.create_engine(
                f"{self._prefix()}://{self._credentials()}@{self.remote_path}", echo=self.echo)
        return engine


class MariaDB(ClientServer):
    """
    MariaDB backend
    """
    def __init__(self, username="root", password="", driver=None,
                 host="127.0.0.1", port=3306, path="cim", echo=False):
        """
        Default constructor for MariaDB backend instance
        :param username: Username for the MariaDB database (default: root)
        :param password: Password for username@MariaDB database (default: "")
        :param driver: Python MariaDB driver (default: mysqlclient)
        :param host: Database host (default: localhost)
        :param port: Database port (default: 3306)
        :param path: Database name (default: "cim")
        :param echo: SQLAlchemy "echo" parameter (default: False)
        """
        super().__init__(username, password, driver, host,
                         port, path, echo)
        self.dialect = "mysql"


class MySQL(ClientServer):
    """
    MySQL backend
    """
    def __init__(self, username="root", password="", driver="pymysql",
                 host="127.0.0.1", port=3306, path="cim", echo=False):
        """
        Default constructor for MySQL backend instance
        :param username: Username for the MySQL database (default: root)
        :param password: Password for username@MySQL database (default: "")
        :param driver: Python MariaDB driver (default: pymysql)
        :param host: Database host (default: localhost)
        :param port: Database port (default: 3306)
        :param path: Database name (default: "cim")
        :param echo: SQLAlchemy "echo" parameter (default: False)
        """
        super().__init__(username, password, driver, host,
                         port, path, echo)
        self.dialect = "mysql"
