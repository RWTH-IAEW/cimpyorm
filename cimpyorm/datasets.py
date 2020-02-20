#
#  Copyright (c) 2018 - 2019 Thomas Offergeld (offergeld@ifht.rwth-aachen.de)
#  Institute for High Voltage Technology
#  RWTH Aachen University
#
#  This module is part of cimpyorm.
#
#  cimpyorm is licensed under the BSD-3-Clause license.
#  For further information see LICENSE in the project's root directory.
#
import os

from cimpyorm import parse, load, get_path


def ENTSOE_FullGrid(refresh=False):
    if not refresh:
        try:
            return load(os.path.join(get_path("DATASETROOT"), "FullGrid", "out.db"))
        except FileNotFoundError:
            return parse(os.path.join(get_path("DATASETROOT"), "FullGrid"))
    else:
        return parse(os.path.join(get_path("DATASETROOT"), "FullGrid"))


def ENTSOE_MiniBB(refresh=False):
    if not refresh:
        try:
            return load(os.path.join(get_path("DATASETROOT"), "MiniGrid_BusBranch", "out.db"))
        except FileNotFoundError:
            return parse(os.path.join(get_path("DATASETROOT"), "MiniGrid_BusBranch"))
    else:
        return parse(os.path.join(get_path("DATASETROOT"), "MiniGrid_BusBranch"))


def ENTSOE_MiniNB(refresh=False):
    if not refresh:
        try:
            return load(os.path.join(get_path("DATASETROOT"), "MiniGrid_NodeBreaker", "out.db"))
        except FileNotFoundError:
            return parse(os.path.join(get_path("DATASETROOT"), "MiniGrid_NodeBreaker"))
    else:
        return parse(os.path.join(get_path("DATASETROOT"), "MiniGrid_NodeBreaker"))
