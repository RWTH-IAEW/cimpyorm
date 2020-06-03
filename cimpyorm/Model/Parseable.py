#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

from cimpyorm.auxiliary import XPath


class Parseable:
    """
    Base class for CIM classes that are to be parsed from CIM instance, providing
    parse methods for static (rdf:ID) objects and supplementary (rdf:about)
    information.
    """
    Map = {}
    _about_ref = None
    ObjectName = None
    _schema_class = None

    def __str__(self):
        _str = [f"{self._schema_class.name} object with the following properties:"]
        for prop in self._schema_class.props:
            _str.append(prop.label)
        return r"\n".join(_str)

    @classmethod
    def fields(cls):
        """
        Print information about available fields in Class
        :return: None
        """
        print(f"Fields available for class {cls.__name__}")
        [print(var) for var in vars(cls).keys() if not var.startswith("_")]  # pylint: disable=expression-not-assigned

    @classmethod
    def describe(cls, fmt="psql"):
        cls._schema_class.describe(fmt)

    @classmethod
    def to_html(cls, **kwargs):
        return cls._schema_class.to_html(**kwargs)
