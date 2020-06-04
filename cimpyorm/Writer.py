#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

from datetime import datetime
import uuid
from abc import abstractmethod
from json import loads
from itertools import chain

# This module creates Elements and ElementTrees to be serialized by using internal objects. Since no external
# entities are deserialized, this should generally be secure.
from lxml.etree import Element, SubElement, ElementTree     # nosec
from sqlalchemy.orm import Bundle
from sqlalchemy import or_
from tqdm import tqdm

from cimpyorm.Model.Elements.Enum import CIMEnum
from cimpyorm.Model.Elements.Class import CIMClass
from cimpyorm.Model.Elements.Property import CIMProp, CIMProp_AlphaNumeric, CIMProp_Reference, \
    CIMProp_Enumeration
from cimpyorm.Model.Elements.Base import CIMProfile
from cimpyorm.auxiliary import get_logger, DEFAULTS

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
        self.root = Element("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF",
                               nsmap=NAMESPACES)

    @abstractmethod
    def build_tree(self, profiles=None):
        raise NotImplementedError

    def serialize_fullmodel_object(self, profiles=None, uuids=None, header_data=None):
        """
        Generate the FullModel Object contained in the object-tree and add it to the root
        """
        if isinstance(profiles, str):
            profiles = (profiles,)

        if not profiles:
            profiles = self.dataset.query(CIMProfile)
        else:
            profiles = self.dataset.query(CIMProfile).filter(or_(CIMProfile.name.in_(profiles),
                                                                 CIMProfile.short.in_(profiles)))

        uris = (loads(profile.uri).values() for profile in profiles)
        uris = list(chain(*uris))

        MD = "{http://iec.ch/TC57/61970-552/ModelDescription/1#}"
        if not uuids:
            _uuid = f"urn:uuid:{str(uuid.uuid4())}"
        else:
            if not profiles.count() == 1:
                raise ValueError
            _uuid = uuids[profiles[0].name]
        fm = SubElement(self.root, f"{MD}FullModel",
                           {
                               "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about":
                                   f"urn:uuid:{_uuid}"
                           })
        if self.dataset.mas:
            SubElement(fm, f"{MD}Model.modelingAuthoritySet").text = str(self.dataset.mas)
        else:
            SubElement(fm, f"{MD}Model.modelingAuthoritySet").text = "CIMPyORM-Export"
        if self.dataset.scenario_time:
            SubElement(fm, f"{MD}Model.scenarioTime").text = str(self.dataset.scenario_time)
        if header_data and "profile_header" in header_data:
            if not isinstance(header_data["profile_header"], (list, tuple)):
                raise ValueError("Invalid structure for profile_header.")
            else:
                no_profile_in_header = True
                for _p in header_data["profile_header"]:
                    if _p in uris:
                        SubElement(fm, f"{MD}Model.profile").text = str(_p)
                        no_profile_in_header = False
                    else:
                        log.debug(f"Profile header '{str(_p)}' not defined in profiles URIs.")
                if no_profile_in_header:
                    log.warning(f"No profile defined in file header for file containing "
                                f"{[profile.short for profile in profiles]} due to manual "
                                f"profile_header filter selection.")
        else:
            for uri in uris:
                SubElement(fm, f"{MD}Model.profile").text = str(uri)
        if uuids:
            for profile in profiles:
                for dep in profile.mandatory_dependencies:
                    SubElement(fm, f"{MD}Model.DependentOn",
                                  {
                                      "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about":
                                          f"urn:uuid:{uuids[dep.name]}"
                                  })
                for dep in profile.optional_dependencies:
                    try:
                        SubElement(fm, f"{MD}Model.DependentOn",
                                      {
                                          "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about":
                                              f"urn:uuid:{uuids[dep.name]}"
                                      })
                    except KeyError:
                        # Optional dependency. This profile was not created, hence it is not
                        # referenced in the header
                        pass
        NOW = datetime.now().isoformat(timespec="seconds") + "Z"
        SubElement(fm, f"{MD}Model.created").text = NOW

    def serialize_single_object(self, object, profiles=None):
        """
        Serialize a single object in the dataset using its ORM instance (which is much slower
        than using raw database queries as in serialize_class_objects) but works with x-to-many
        relationships.

        :param object: The CIM-Object to serialize
        """
        _c = object.__class__._schema_class
        object_prefix = f"{{{NAMESPACES[_c.namespace.short]}}}" \
            if _c.namespace.short != DEFAULTS.Namespace else ""
        el = SubElement(self.root, f"{object_prefix}{_c.name}",
                           {f"{{{NAMESPACES['rdf']}}}ID": f"{object.id}"})
        for name, prop in _c.all_props.items():
            try:
                if prop.used:
                    prop_prefix = f"{{{NAMESPACES[prop.namespace.short]}}}" \
                        if prop.namespace.short != DEFAULTS.Namespace else ""
                    prop_cls = prop.cls.name
                    if not prop.range:
                        # This is a value prop (not a reference)
                        val = getattr(object, name)
                        if val:
                            xml_prop = SubElement(
                                el,
                                f"{prop_prefix}{prop_cls}.{prop.name}")
                            xml_prop.text = xml_valid_value(val)
                    elif isinstance(prop.range, CIMEnum):
                        # This value is a reference to an Enum-Value
                        val = getattr(object, f"{name}_name")
                        if val:
                            SubElement(el, f"{prop_prefix}{prop_cls}.{prop.name}",
                                          {f"{{{NAMESPACES['rdf']}}}resource": f"#{val}"})
                    elif prop.range:
                        # This value is a reference
                        if prop.many_remote:
                            values = (val.id for val in getattr(object, f"{name}"))
                            [SubElement(el, f"{prop_prefix}{prop_cls}.{prop.name}",
                                           {f"{{{NAMESPACES['rdf']}}}resource": f"#{val}"}) for val
                             in values]
                        else:
                            val = getattr(object, f"{name}_id")
                            # Significantly faster than getattr(obj, "name").id
                            if val:
                                SubElement(el, f"{prop_prefix}{prop_cls}.{prop.name}",
                                              {f"{{{NAMESPACES['rdf']}}}resource": f"#{val}"})
            except AttributeError:
                log.error(f"Error parsing property {prop.name} of {_c.name}")

    def serialize_class_objects(self, class_, profiles=None):
        """
        Serialize the objects in the dataset for a single CIM class.

        :param class_: The CIMClass Object to look for.
        :param profiles: The profiles to serialize for.
        """
        properties = class_.serialized_properties()
        if profiles:
            c_defined_in = class_.defined_in in profiles
            c_pref = {True: "ID", False: "about"}[c_defined_in]
            # Filter by profiles
            if c_defined_in:
                properties = {k: v for k, v in properties.items()
                              if set(p.name for p in k.allowed_in) & set(profiles)}
            else:
                properties = {k: v for k, v in properties.items()
                              if k.defined_in in profiles}
                if not properties:
                    return
        else:
            c_pref = "ID"
        bundle = DictBundle("attrs",
                            *[getattr(class_.class_, attr) for attr in properties.values()])
        if properties:
            records = self.dataset.query(class_.class_.id, bundle).filter(
                class_.class_.type_ == class_.full_name).all()
        else:
            records = self.dataset.query(class_.class_.id).filter(
                class_.class_.type_ == class_.full_name).all()
            if records:
                # If no properties are defined, this query should return empty.
                raise ValueError

        for id, record in records:
            object_prefix = f"{{{NAMESPACES[class_.namespace.short]}}}" \
                if class_.namespace.short != DEFAULTS.Namespace else ""
            s_id = f"{id}" if c_pref=="ID" else f"#{id}"
            el = SubElement(self.root, f"{object_prefix}{class_.name}",
                               {f"{{{NAMESPACES['rdf']}}}{c_pref}": s_id})
            for prop, (k, v) in zip(properties.keys(), record.items()):
                if v is not None:
                    prop_prefix = f"{{{NAMESPACES[prop.namespace.short]}}}" \
                        if prop.namespace.short != DEFAULTS.Namespace else ""
                    attrname = f"{prop_prefix}{prop.cls.name}.{prop.name}"
                    if isinstance(prop, CIMProp_AlphaNumeric):
                        SubElement(el, attrname).text = xml_valid_value(v)
                    elif isinstance(prop, CIMProp_Reference):
                        SubElement(el, attrname, {f"{{{NAMESPACES['rdf']}}}resource": f"#{v}"})
                    elif isinstance(prop, CIMProp_Enumeration):
                        # Remove the namespace prefix stored in the database. We will prepend the
                        # full namespace identifier anyway
                        v = v.split("_")[-1]
                        SubElement(el, attrname,
                                      {f"{{{NAMESPACES['rdf']}}}resource":
                                       f"{prop.namespace.full_name}"
                                       f"{prop.range.name}.{v}"})


class SingleFileSerializer(Serializer):

    def build_tree(self, profiles=None, uuids=None, header_data=None):
        """
        Serialize the dataset.

        :param profiles: Either a single profile identifier (str), or an iterable (not str) of
        profile identifiers to be combined into a single tree.

        :param uuids: A map of profile uuids to map the profile-to-profile dependencies in the
        FullModel Objects.

        :return: The ElementTree representation of the dataset.
        """
        if isinstance(profiles, str):
            profiles = (profiles,)
        self.serialize_fullmodel_object(profiles, uuids, header_data)
        if profiles:
            classes = self.dataset.query(CIMClass).join(CIMProfile,
                                                        CIMClass.used_in).filter(
                CIMProfile.name.in_(profiles)).all()
        else:
            classes = self.dataset.query(CIMClass).all()
        for _class in tqdm(classes, desc="Generating XML",
                           total=len(classes)):
            if any([prop.used and prop.many_remote for prop in _class.props]):
                # Fall back to using ORM for classes that have many_remote properties
                objects = self.dataset.query(_class.class_)
                [self.serialize_single_object(obj, profiles) for obj in objects]
            else:
                self.serialize_class_objects(_class, profiles)
        return ElementTree(self.root)


class MultiFileSerializer(Serializer):

    def build_tree(self, profiles=None, header_data=None):
        """
        Serialize the dataset.

        :return: The ElementTree representation of the dataset.
        """
        if not profiles:
            raise ValueError("The MultiFileSerializer needs a list of profiles to split the "
                             "dataset.")
        profile_db = self.dataset.query(CIMProfile).filter(or_(CIMProfile.name.in_(profiles),
                                                               CIMProfile.short.in_(profiles)))
        uuids = {profile.name: str(uuid.uuid4()) for profile in profile_db}
        forest = [SingleFileSerializer(self.dataset).build_tree(profiles=profile, uuids=uuids,
                                                                header_data=header_data)
                  for profile in profiles]
        return forest


def xml_valid_value(v):
    if v is True:
        return "true"
    elif v is False:
        return "false"
    else:
        return str(v)


class DictBundle(Bundle):
    """
    Turn a SQLAlchemy ORM Bundle into a dict
    """
    def create_row_processor(self, query, procs, labels):
        """Override create_row_processor to return values as dictionaries"""
        def proc(row):
            return dict(zip(labels, (proc(row) for proc in procs)))
        return proc
