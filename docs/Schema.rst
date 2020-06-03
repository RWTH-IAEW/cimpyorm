==============
Schema
==============
The CIM Elements are organized in profiles and namespaces:

.. autoclass:: cimpyorm.Model.Elements.Base.CIMProfile
    :members:

.. autoclass:: cimpyorm.Model.Elements.Base.CIMNamespace
    :members:

**************
SchemaElements
**************

All elements described in a CIM Schema inherit from a common mixin (SchemaElement), that provides
some common functionality, such as relationships to
:py:class:`~cimpyorm.Model.Elements.Base.CIMProfile` and
:py:class:`~cimpyorm.Model.Elements.Base.CIMNamespace` objects.

.. autoclass:: cimpyorm.Model.Elements.Base.SchemaElement

    .. py:attribute:: profile

        The profile this element was defined in.

    .. py:attribute:: namespace

        The namespace this element is associated with.


++++++++++++++
CIMClass
++++++++++++++

The CIM Class represents a class defined in the CIM Schema.

.. autoclass:: cimpyorm.Model.Elements.Class.CIMClass
    :members:


++++++++++++++
CIMProp
++++++++++++++

The CIM Class represents a class defined in the CIM Schema.

.. autoclass:: cimpyorm.Model.Elements.Property.CIMProp
    :members:
