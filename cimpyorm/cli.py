#
#  Copyright (c) 2018 - 2019 Thomas Offergeld (offergeld@ifht.rwth-aachen.de)
#  Institute for High Voltage Technology
#  RWTH Aachen University
#
#  This module is part of cimpyorm.
#
#  CIMPy is licensed under the BSD-3-Clause license.
#  For further information see LICENSE in the project's root directory.
#
import click

import cimpyorm
from cimpyorm.backends import SQLite


@click.group()
def cli():
    pass


@cli.command()
@click.argument("path_to_db", type=click.Path(exists=True))
@click.option("--echo/--no-echo", "echo", default=False)
def load(path_to_db, echo=False):
    """
    Load an already parsed database from disk or connect to a server and yield a database session to start querying on
    with the classes defined in the model namespace.

    Afterwards, the database can be queried using SQLAlchemy query syntax, providing the CIM classes contained in the
    :class:`~argparse.Namespace` return value.

    :param path_to_db: Path to the cim snapshot or a :class:`~cimpyorm.backend.Engine`.
    :param echo: Echo the SQL sent to the backend engine (SQLAlchemy option).
    """

    cimpyorm.load(path_to_db, echo)


@cli.command()
@click.argument("dataset", type=click.Path(exists=True))
def parse(dataset):
    """
    Parse a database into a database backend and yield a database session to start querying on with the classes defined
    in the model namespace.

    Afterwards, the database can be queried using SQLAlchemy query syntax, providing the CIM classes contained in the
    :class:`~argparse.Namespace` return value.

    :param dataset: Path to the cim snapshot.
    :param backend: Database backend to be used (defaults to a SQLite on-disk database in the dataset location).
    """

    cimpyorm.parse(dataset, SQLite())
