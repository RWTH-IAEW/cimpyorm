#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

from itertools import chain
from collections import defaultdict
from functools import lru_cache

from tqdm import tqdm

from cimpyorm.auxiliary import HDict, get_logger, parseable_files, shorten_namespace

log = get_logger(__name__)


def get_files(dataset):
    if isinstance(dataset, list):
        files = chain(*[parseable_files(path) for path in dataset])
    else:
        files = parseable_files(dataset)
    return files


def merge_sources(sources, model_schema=None):
    """
    Merge different sources of CIM datasets (usually the different profiles, but could also be
    multiple instances of the same profile when multiple datasets are merged via boundary datasets)

    :param sources: SourceInfo objects of the source files.
    :param model_schema: The schema used to deserialize the dataset.

    :return: A dictionary of the objects found in the dataset, keyed by classname and object uuid.
    """
    uuid2name = dict()
    uuid2data = dict()

    classname_list = defaultdict(set)

    from cimpyorm.auxiliary import XPath
    xp = {"id": XPath("@rdf:ID", namespaces=get_nsmap(sources)),
          "about": XPath("@rdf:about", namespaces=get_nsmap(sources))}
    for source in sources:
        for element in source.tree.getroot():
            try:
                uuid = determine_uuid(element, xp)
                classname = shorten_namespace(element.tag, HDict(get_nsmap(sources)))

                # Set the classname only when UUID is attribute
                try:
                    uuid = xp["id"](element)[0]
                    if uuid in uuid2name and uuid2name[uuid] != classname:
                        # If multiple objects of different class share the same uuid, raise an Error
                        raise ReferenceError(f"uuid {uuid}={classname} already defined as {uuid2name[uuid]}")

                    uuid2name[uuid] = classname
                except IndexError:
                    pass

                classname_list[uuid] |= {classname}

                if uuid not in uuid2data:
                    uuid2data[uuid] = element
                else:
                    [uuid2data[uuid].append(sub) for sub in element]  # pylint: disable=expression-not-assigned
            except ValueError:
                log.warning(f"Skipped element during merge: {element}.")

    # print warning in case uuid references use different classnames
    for uuid, name_set in classname_list.items():
        if len(name_set) > 1:
            log.warning(f"Ambiguous classnames for {uuid} of type {uuid2name.get(uuid, None)} = {name_set}")

    # check that the class is the most specific one in the list
    if model_schema is not None:
        schema_classes = model_schema.get_classes()
        for uuid, classname in uuid2name.items():
            try:
                cls = schema_classes[classname]
            except KeyError:
                log.info(f"Class {classname} is not included in schema. Objects of this class are not deserialized.")
            else:
                try:
                    if not all(issubclass(cls, schema_classes[_cname]) for _cname in classname_list[uuid]):
                        raise ValueError(f"Class {classname} is not most specific of {classname_list[uuid]}.")
                except KeyError as ex:
                    raise ReferenceError(f"Malformed schema. Class-hierarchy-element is missing: {ex}.")

    # transform the data into output structure
    d_ = defaultdict(dict)
    for uuid, classname in uuid2name.items():
        d_[classname][uuid] = uuid2data[uuid]

    return d_


def parse_entries(entries, schema, silence_tqdm=False):
    classes = dict(schema.session.query(
        schema.Element_classes["CIMClass"].name,
        schema.Element_classes["CIMClass"]
    ).all())
    # Fixme: Need to use full_name, otherwise conflicts are dropped silently
    created = []
    for classname, elements in entries.items():
        if classname in classes.keys():
            for uuid, element in tqdm(elements.items(), desc=f"Reading {classname}", leave=False,
                                      disable=silence_tqdm):
                argmap, insertables = classes[classname].parse_values(element, schema.session)
                created.append(classes[classname].class_(id=uuid,
                                                         **argmap))
                for insertable in insertables:
                    schema.session.execute(insertable)
        else:
            log.info(f"{classname} not implemented. Skipping.")
    return created


def determine_uuid(element, xp):
    uuid = None
    try:
        uuid = xp["id"](element)[0]
    except IndexError:
        pass
    try:
        uuid = xp["about"](element)[0].split("urn:uuid:")[-1].split("#")[-1]
    except IndexError:
        pass
    return uuid


@lru_cache()
def get_nsmap(sources: frozenset):
    """
    Return the merged namespace_name map for a list of data sources
    :param sources: frozenset of DataSource objects (so its hashable)
    :return: dict, merged nsmap of all DataSource objects
    """
    nsmaps = [source.nsmap for source in sources]
    nsmaps = {k: v for d in nsmaps for k, v in d.items()}
    return HDict(nsmaps)


def get_cim_version(sources):
    """
    Return the (unambiguous) DataSource cim versions
    :param sources: DataSources
    :return:
    """
    cim_versions = [source.cim_version for source in sources]
    if len(set(cim_versions)) > 1:
        log.error(f"Ambiguous cim_versions: {cim_versions}.")
    return cim_versions[0]

