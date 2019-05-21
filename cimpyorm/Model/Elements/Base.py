from typing import Union

from lxml import etree
from lxml.etree import XPath
from sqlalchemy import Column, String, ForeignKey

from cimpyorm.auxiliary import log
from cimpyorm.Model import auxiliary as aux


def prefix_ns(func):
    """
    Prefixes a property return value with the elements xml-namespace (if its not the default namespace "cim").

    Creates unique labels for properties and classes.
    """
    def wrapper(obj):
        """
        :param obj: Object that implements the namespace property (E.g. CIMClass/CIMProp)
        :return: Representation with substituted namespace
        """
        s = func(obj)
        res = []
        if s and isinstance(s, list):
            for element in s:
                if element.startswith("#"):
                    element = "".join(element.split("#")[1:])
                for key, value in obj.nsmap.items():
                    if value in element:
                        element = element.replace(value, key+"_")
                res.append(element)
        elif s:
            if s.startswith("#"):
                s = "".join(s.split("#")[1:])
            for key, value in obj.nsmap.items():
                if value in s:
                    s = s.replace(value, key + "_")
            res = s
        else:
            res = None
        return res
    return wrapper


class SchemaElement(aux.Base):
    """
    ABC for schema entities.
    """
    __tablename__ = "SchemaElement"
    nsmap = None
    XPathMap = None
    name = Column(String(80), primary_key=True)
    label = Column(String(50))
    namespace = Column(String(30))
    type_ = Column(String(50))
    #comment = Column(String(300))

    __mapper_args__ = {
        "polymorphic_on": type_,
        "polymorphic_identity": __tablename__
    }

    def __init__(self, description=None):
        """
        The ABC's constructor
        :param description: the (merged) xml node element containing the class's description
        """
        if description is None:
            log.error(f"Initialisation of CIM model entity without associated "
                      f"description invalid.")
            raise ValueError(f"Initialisation of CIM model entity without "
                             f"associated description invalid.")
        self.description = description
        self.Attributes = self._raw_Attributes()
        self.name = self._name
        self.label = self._label
        self.namespace = self._namespace
        self.Map = None

    @staticmethod
    def _raw_Attributes():
        return {"name": None, "label": None, "namespace": None}

    @classmethod
    def _generateXPathMap(cls):
        """
        Generator for compiled XPath expressions (those require a namespace map to be present, hence they are compiled
        at runtime)
        :return: None
        """
        cls.XPathMap = {"label": XPath(r"rdfs:label/text()", namespaces=cls.nsmap)}
        return cls.XPathMap

    @property
    @prefix_ns
    def _label(self):
        """
        Return the class' label
        :return: str
        """
        return self._raw_property("label")

    @property
    def full_label(self):
        if not self.namespace or self.namespace=="cim":
            return self.label
        else:
            return self.namespace+"_"+self.label

    @property
    def _namespace(self) -> Union[str, None]:
        if not self.Attributes["namespace"]:
            if not any(self.name.startswith(ns+"_") for ns in self.nsmap.keys()):
                self.Attributes["namespace"] = "cim"
            else:
                self.Attributes["namespace"] = self.name.split("_")[0]
        return self.Attributes["namespace"]

    @property
    def _comment(self):
        """
        Return the class' label
        :return: str
        """
        # Fixme: This is very slow and not very nice (each string contains the entire xml header - parsing xpath(
        #  "*/text()) doesn't work due to the text containing xml tags). Therefore, this is currently disabled
        str_ = "".join(str(etree.tostring(content, pretty_print=True)) for content in self.description.xpath(
            "rdfs:comment", namespaces=self.nsmap))
        return str_

    @property
    @prefix_ns
    def _name(self) -> Union[str, None]:
        """
        Accessor for an entities name (with cache)
        :return: The entities name as defined in its description
        """
        if self.Attributes["name"]:
            pass
        else:
            _n = self.description.values()[0]
            self.Attributes["name"] = _n
        self.name = self.Attributes["name"]
        return self.Attributes["name"]

    def _raw_property(self, property_identifier) -> Union[list, str, None]:
        """
        Extract a property from the CIM entity
        :param property_identifier: property name
        :return: The CIM entity's property as a list, a string, or None
        """
        if self.Attributes[property_identifier] is None:
            xp = self.XPathMap
            if property_identifier not in xp.keys():
                raise KeyError(f"Invalid property_identifier name {property_identifier}.")
            results = xp[property_identifier](self.description)  # pylint: disable=unsubscriptable-object
            if len(set(results)) == 1:
                self.Attributes[property_identifier] = results[0]
            elif not results:
                self.Attributes[property_identifier] = None
            else:
                log.warning(f"Ambiguous class property_identifier ({property_identifier}) for {self.name}.")
                self.Attributes[property_identifier] = [result for result in set(results)]
        return self.Attributes[property_identifier]

    def describe(self, fmt="psql"):
        print(self)


class CIMPackage(SchemaElement):
    __tablename__ = "CIMPackage"
    name = Column(String(80), ForeignKey(SchemaElement.name), primary_key=True)

    __mapper_args__ = {
        "polymorphic_identity": __tablename__
    }

    def __init__(self, description):
        """
        Class constructor
        :param description: the (merged) xml node element containing the package's description
        """
        super().__init__(description)
