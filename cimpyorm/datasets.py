#   Copyright (c) 2018 - 2020 Institute for High Voltage Technology and Institute for High Voltage Equipment and Grids, Digitalization and Power Economics
#   RWTH Aachen University
#   Contact: Thomas Offergeld (t.offergeld@iaew.rwth-aachen.de)
#  #
#   This module is part of CIMPyORM.
#  #
#   CIMPyORM is licensed under the BSD-3-Clause license.
#   For further information see LICENSE in the project's root directory.
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
