#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
#

import pytest
import lxml.etree as et
import os

from cimpyorm.auxiliary import log, get_path, parseable_files

schemata = []
datasets = []
try:
    SCHEMAROOT = get_path("SCHEMAROOT")
    if os.path.isdir(SCHEMAROOT) and os.listdir(SCHEMAROOT):
        schemata = [os.path.join(SCHEMAROOT, f"CIM{version}")
                    for version in [16]]
except KeyError:
    pass

try:
    DATASETROOT = get_path("DATASETROOT")
    if os.path.isdir(os.path.join(DATASETROOT)) and os.listdir(os.path.join(DATASETROOT)):
        datasets = [os.path.join(DATASETROOT, dir_) for dir_ in os.listdir(os.path.join(DATASETROOT))
                    if os.path.isdir(os.path.join(DATASETROOT, dir_))]
except KeyError:
    pass

tested_directories = schemata + datasets

# ToDo: These tests are depcreated as merge() is no longer used. Remove them.

# @pytest.mark.parametrize("path", tested_directories)
# def test_count_merged_elements(path):
#     """
#     Make sure no information is lost during XMLMerge (Equal number of elements).
#     :param path: Path to folder containing xml files.
#     :return:
#     """
#     files = os.listdir(path)
#     files = [os.path.join(path, file) for file in files if
#              file.endswith(".xml") or file.endswith(".rdf")]
#     elements = 0
#     for xmlfile in files:
#         elements += len(et.parse(xmlfile).getroot())
#     tree = merge(path)
#     assert len(tree.getroot()) == elements
#
#
# @pytest.mark.parametrize("path", tested_directories)
# def test_count_properties(path):
#     files = os.listdir(path)
#     files = [os.path.join(path, file) for file in files if
#              file.endswith(".xml") or file.endswith(".rdf")]
#     properties_unmerged = 0
#     for xmlfile in files:
#         for node in et.parse(xmlfile).getroot():
#             properties_unmerged += len(node)
#     tree = merge(path)
#     properties_merged = sum(len(node) for node in tree.getroot())
#     assert properties_merged == properties_unmerged
#
#
# @pytest.mark.parametrize("path", tested_directories)
# def test_merged_nsmaps(path):
#     expected = {}
#     for file in parseable_files(path):
#         for key, value in et.parse(file).getroot().nsmap.items():
#             expected[key] = value
#     tree = merge(path)
#     log.info(f"{len(expected.keys())} entries expected in nsmap. {len(tree.getroot().nsmap.keys())} found")
#     log.debug(f"Expected: {expected.keys()}")
#     log.debug(f"Found: {tree.getroot().nsmap.keys()}")
#     assert tree.getroot().nsmap == expected
