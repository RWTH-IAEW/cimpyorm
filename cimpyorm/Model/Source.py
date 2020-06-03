#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Union
from functools import lru_cache

from defusedxml.lxml import parse
from sqlalchemy import Column, Integer, String, TEXT

from cimpyorm.auxiliary import HDict
import cimpyorm.Model.auxiliary as aux


class SourceInfo(aux.Base):
    """
    Class for storing source metadata in the database
    """
    __tablename__ = "SourceInfo"
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(50))
    uuid = Column(String(50))
    FullModel = Column(TEXT)
    namespaces = Column(TEXT)

    def __init__(self, source_file):
        """
        Initialize DataSource object
        :param source_file: Path to the file containing the model data
        """
        self.source = source_file
        self._parse_meta()

    def __repr__(self):
        """
        Unique representation
        :return: str
        """
        fm = json.loads(self.FullModel)
        str_ = f"source uuid: {self.uuid} | filename: {self.filename} | profiles: {fm['profile_name']}"
        return str_

    @property
    def cim_version(self):
        """
        Return the source's cim_version
        :return: str - The source's cim version
        """
        nsmap = HDict(json.loads(self.namespaces))
        return _get_cimrdf_version(nsmap["cim"])

    @property
    @lru_cache()
    def nsmap(self):
        """
        Return the source's nsmap
        :return: dict - The source's nsmap
        """
        nsmap = HDict(json.loads(self.namespaces))
        return nsmap

    def _parse_meta(self):
        try:
            self.filename = Path(self.source).name
        except TypeError:
            self.filename = self.source.name
        self.tree = parse(self.source)
        root = self.tree.getroot()
        nsmap = root.nsmap
        uuid, metadata = self._generate_metadata()
        self.uuid = uuid
        self.FullModel = json.dumps(metadata)
        self.namespaces = json.dumps(nsmap)

    def _generate_metadata(self):
        """
        Determine the data source's metadata (such as CIM version)
        :return: (data source uuid, data source metadata)
        """
        tree = self.tree
        nsmap = tree.getroot().nsmap
        try:
            source = tree.xpath("md:FullModel", namespaces=nsmap)[0]
        except IndexError:
            # No FullModel instance present.
            return None, None
        full_model_id = set(source.xpath("@rdf:about", namespaces=nsmap)) | set(
            source.xpath("@rdf:ID", namespaces=nsmap))
        if not len(full_model_id) == 1:
            raise ValueError("Ambiguous model ID.")
        uuid = list(full_model_id)[0].split("urn:uuid:")[-1]
        metadata = defaultdict(list)
        for element in source:
            entry = element.tag.split("Model.")[-1]
            value = element.text if entry != "DependentOn" else \
            element.attrib.values()[0].split("urn:uuid:")[-1]
            if value not in metadata[entry]:
                metadata[entry].append(value)
        return uuid, metadata


def _get_cimrdf_version(cim_ns) -> Union[None, str]:
    """
    Parse the cim namespace_name into a version number
    :param cim_ns: cim namespace_name
    :return: double, version number, or None if no version could be identified
    """
    match = re.search(r"(?<=CIM-schema-cim)\d{0,2}?(?=#)", cim_ns)
    if match:
        return match.group()
    else:
        return None
