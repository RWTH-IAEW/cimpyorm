=======================
Serialization (Export)
=======================
CIMPyORM can be used to serialize datasets into XML-files as defined by the CIM standard.
This way, datasets can be exported from CIMPyORM to facilitate import into other tools.

************************
Dataset preparation
************************
Before serialization, a dataset needs to be created and populated with CIM objects.
To do this, either parse a dataset (as described in :any:`quickstart`), or start
with an
empty dataset and fill it with your own objects::

    dataset, model = cimpyorm.create_empty_dataset(version="16")

Either add single objects::

    term1 = model.Terminal(id=42, phases=model.enum.PhaseCode.v.AB)
    dataset.add(term1)

Or add multiple at once::

    dataset.add_all((model.Terminal(id=id) for id in range(10))

And finally commit the changes to the database::

    dataset.commit()

***************************
Export
***************************
Once your dataset is ready to be exported, you can use the ``export`` function to generate the
XML representation of the dataset as an InMemory-BytesIO object that can be written to disk or
streamed.

.. autofunction:: cimpyorm.export

The result can be written to disk by::

    with open("outfile.xml", "wb+") as f:   # Open the file handle in binary mode with write permissions
        f.write(result.getvalue())

