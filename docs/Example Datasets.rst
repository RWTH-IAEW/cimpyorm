================
Example Datasets
================
CIMPyORM comes preloaded with the ENTSO-E FullGrid and MiniGrid datasets.

They can be accessed through the datasets module::

    from cimpyorm import datasets
    db_session, model = datasets.ENTSOE_FullGrid()
    db_session, model = datasets.ENTSOE_MiniBB()    # The Bus-Branch Model version
    db_session, model = datasets.ENTSOE_MiniNB()    # The Node-Breaker Model version

