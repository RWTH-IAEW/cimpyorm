#
#  Copyright (c) 2018 - 2018 Thomas Offergeld (offergeld@ifht.rwth-aachen.de)
#  Institute for High Voltage Technology
#  RWTH Aachen University
#
#  This module is part of cimpyorm.
#
#  cimpyorm is licensed under the BSD-3-Clause license.
#  For further information see LICENSE in the project's root directory.
#

from lxml.etree import XPath


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

    @classmethod
    def compile_map(cls, nsmap):
        """
        Compile the XPath map for the parsing run
        :param nsmap: The .xml nsmap
        :return: None
        """
        attribute_map = cls.Map
        for key, element in cls.Map.items():
            if key not in cls.__bases__[0].Map:  # pylint: disable=no-member
                attribute_map[key] = XPath(element, namespaces=nsmap)
        cls.Map = attribute_map

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
