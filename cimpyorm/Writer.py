#
#  Copyright (c) 2018 - 2020 Thomas Offergeld (offergeld@ifht.rwth-aachen.de)
#  Institute for High Voltage Technology
#  RWTH Aachen University
#
#  This module is part of CIMPy.
#
#  CIMPy is licensed under the BSD-3-Clause license.
#  For further information see LICENSE in the project's root directory.
#

from datetime import datetime
import uuid

from lxml import etree as et
from sqlalchemy.orm import Bundle
from tqdm import tqdm

from cimpyorm.Model.Elements import CIMEnum, CIMClass
from cimpyorm.auxiliary import get_logger

log = get_logger(__name__)

NAMESPACES = {
    "cim": "http://iec.ch/TC57/2013/CIM-schema-cim16#",
    "entsoe": "http://entsoe.eu/CIM/SchemaExtension/3/1#",
    "md": "http://iec.ch/TC57/61970-552/ModelDescription/1#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}


class Serializer:
    def __init__(self, dataset=None):
        """
        Initialize a Serializer instance to serialize the dataset into CIM-XML.

        :param dataset: The dataset to be serialized.
        """
        self.root = None
        self.dataset = dataset
        self.root = et.Element("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF",
                               nsmap=NAMESPACES)

    def serialize(self):
        """
        Serialize the dataset.

        :return: The ElementTree representation of the dataset.
        """
        # Fixme: If this returns the ET, then serialize() is the wrong name for this function -->
        #  Return an actual serialization (BytesIO or similar)
        self.serialize_fullmodel_object()
        for _class in tqdm(self.dataset.query(CIMClass).all(), desc="Generating XML",
                           total=self.dataset.query(CIMClass).count()):
            if any([prop.used and prop.many_remote for prop in _class.props]):
                # Fall back to using ORM for classes that have many_remote properties
                objects = self.dataset.query(_class.class_)
                [self.serialize_single_object(obj) for obj in objects]
            else:
                self.serialize_class_objects(_class)
        return et.ElementTree(self.root)

    def serialize_fullmodel_object(self):
        """
        Generate the FullModel Object contained in the object-tree and add it to the root
        """
        MD = "{http://iec.ch/TC57/61970-552/ModelDescription/1#}"
        _uuid = f"urn:uuid:{str(uuid.uuid4())}"
        fm = et.SubElement(self.root, f"{MD}FullModel",
                           {"{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about": _uuid})
        if self.dataset.mas:
            et.SubElement(fm, f"{MD}modelingAuthoritySet").text = str(self.dataset.mas)
        else:
            et.SubElement(fm, f"{MD}modelingAuthoritySet").text = "CIMPyORM-Export"
        if self.dataset.scenario_time:
            et.SubElement(fm, f"{MD}scenarioTime").text = str(self.dataset.scenario_time)
        NOW = datetime.now().isoformat(timespec="seconds") + "Z"
        et.SubElement(fm, f"{MD}created").text = NOW

    def serialize_single_object(self, object):
        """
        Serialize a single object in the dataset using its ORM instance (which is much slower
        than using raw database queries as in serialize_class_objects) but works with x-to-many
        relationships.

        :param object: The CIM-Object to serialize
        """
        _c = object.__class__._schema_class
        el = et.SubElement(self.root, f"{{{NAMESPACES[_c.namespace]}}}{_c.label}",
                           {f"{{{NAMESPACES['rdf']}}}ID": f"{object.id}"})
        for name, prop in _c.all_props.items():
            try:
                if prop.used:
                    if not prop.range:
                        # This is a value prop (not a reference)
                        val = getattr(object, name)
                        if val:
                            xml_prop = et.SubElement(el, f"{{{NAMESPACES[prop.namespace]}}}{prop.name}")
                            xml_prop.text = str(val)
                    elif isinstance(prop.range, CIMEnum):
                        # This value is a reference to an Enum-Value
                        val = getattr(object, f"{name}_name")
                        if val:
                            et.SubElement(el, f"{{{NAMESPACES[prop.namespace]}}}{prop.name}",
                                          {f"{{{NAMESPACES['rdf']}}}resource": f"#{val}"})
                    elif prop.range:
                        # This value is a reference
                        if prop.many_remote:
                            values = (val.id for val in getattr(object, f"{name}"))
                            [et.SubElement(el, f"{{{NAMESPACES[prop.namespace]}}}{prop.name}",
                                           {f"{{{NAMESPACES['rdf']}}}resource": f"#{val}"}) for val
                             in values]
                        else:
                            val = getattr(object, f"{name}_id")
                            # Significantly faster than getattr(obj, "name").id
                            if val:
                                et.SubElement(el, f"{{{NAMESPACES[prop.namespace]}}}{prop.name}",
                                              {f"{{{NAMESPACES['rdf']}}}resource": f"#{val}"})
            except AttributeError:
                log.error(f"Error parsing property {prop.name} of {_c.label}")

    def serialize_class_objects(self, class_):
        """
        Serialize the objects in the dataset for a single CIM class.

        :param class_: The CIMClass Object to look for
        """
        properties = class_.serialized_properties()
        bundle = DictBundle("attrs",
                            *[getattr(class_.class_, attr) for attr in properties.values()])
        if properties:
            records = self.dataset.query(class_.class_.id, bundle).filter(
                class_.class_.type_ == class_.name).all()
        else:
            records = self.dataset.query(class_.class_.id).filter(
                class_.class_.type_ == class_.name).all()
        for id, record in records:
            el = et.SubElement(self.root, f"{{{NAMESPACES[class_.namespace]}}}{class_.label}",
                               {f"{{{NAMESPACES['rdf']}}}ID": f"{id}"})
            for prop, (k, v) in zip(properties.keys(), record.items()):
                if v:
                    attrname = f"{{{NAMESPACES[prop.namespace]}}}{class_.name}.{prop.label}"
                    if not prop.range:
                        et.SubElement(el, attrname).text = str(v)
                    elif prop.range:
                        et.SubElement(el, attrname, {f"{{{NAMESPACES['rdf']}}}resource": f"#{v}"})


class DictBundle(Bundle):
    """
    Turn a SQLAlchemy ORM Bundle into a dict
    """
    def create_row_processor(self, query, procs, labels):
        """Override create_row_processor to return values as dictionaries"""
        def proc(row):
            return dict(
                        zip(labels, (proc(row) for proc in procs))
                    )
        return proc
