================
Datasets
================
The internal representation of a CIM model is contained in the `dataset` object. A `dataset`
wraps the functionality of a `SQLAlchemy-session object <https://docs.sqlalchemy
.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session>`_ for database operations
(`query`, `add`, `commit` and so on).

Dataset objects are created by parsing or loading a CIM model, or by creating an empty dataset.

.. autofunction:: cimpyorm.create_empty_dataset





****************
Example Datasets
****************
CIMPyORM comes preloaded with the ENTSO-E FullGrid and MiniGrid datasets.

They can be accessed through the datasets module::

    from cimpyorm import datasets
    db_session, model = datasets.ENTSOE_FullGrid()
    db_session, model = datasets.ENTSOE_MiniBB()    # The Bus-Branch Model version
    db_session, model = datasets.ENTSOE_MiniNB()    # The Node-Breaker Model version

