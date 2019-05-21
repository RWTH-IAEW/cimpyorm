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
import os

import click
from tabulate import tabulate

import cimpyorm
from cimpyorm.backends import SQLite, InMemory

try:
    from IPython import embed
except ImportError:
    import code

    def embed():
        variables = globals()
        variables.update(locals())
        shell = code.InteractiveConsole(variables)
        shell.interact()


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
    session, model = cimpyorm.load(path_to_db, echo)
    embed()


@cli.command()
@click.argument("dataset", type=click.Path(exists=True))
@click.option("--silence_tqdm/--no-silence_tqdm", "silence_tqdm", default=False)
def parse(dataset, silence_tqdm):
    """
    Parse a dataset into a database backend.

    :param dataset: Path to the cim snapshot.

    :param backend: Database backend to be used (defaults to a SQLite on-disk database in the dataset location).

    :param silence_tqdm: Silence tqdm progress bars
    """

    session, model = cimpyorm.parse(dataset, SQLite(), silence_tqdm)
    embed()


@cli.command()
@click.argument("dataset", type=click.Path(exists=True))
@click.option("--silence_tqdm/--no-silence_tqdm", "silence_tqdm", default=True)
def lint(dataset, silence_tqdm):
    _, ext = os.path.splitext(dataset)
    if os.path.isdir(dataset) or ext == ".zip":
        session, model = cimpyorm.parse(dataset, InMemory(), silence_tqdm=silence_tqdm)
    elif ext == ".db":
        session, model = cimpyorm.load(dataset)
    else:
        raise ValueError("Invalid dataset path.")
    print(cimpyorm.lint(session, model))
    embed()


