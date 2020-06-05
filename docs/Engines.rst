==============
Engines
==============

.. warning::
    This page is partially outdated as of 06/2020. Proceed with care.

The parser can use several database backend engines that serve as wrappers around SQLAlchemy Engines and sessionmakers.

********************
Common functionality
********************

********************
Database engines
********************

The available engines are:

--------------------
SQLite
--------------------

.. autoclass:: cimpyorm.backends.SQLite
    :special-members: __init__

For convenience, there is a named backend for In-Memory SQLite databases:

.. autoclass:: cimpyorm.backends.InMemory
    :special-members: __init__


--------------------
Client-Server
--------------------

~~~~~~~~~~~~~~~~~~~~
MariaDB
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: cimpyorm.backends.MariaDB
    :special-members: __init__

~~~~~~~~~~~~~~~~~~~~
MySQL
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: cimpyorm.backends.MySQL
    :special-members: __init__