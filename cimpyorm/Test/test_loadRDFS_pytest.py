import os
import pytest
from cimpyorm import get_path
from cimpyorm.Model.auxiliary import find_rdfs_path


@pytest.mark.parametrize("Version", [(16)])
def test_find_valid_rdfs_version(Version):
    try:
        os.path.isdir(get_path("SCHEMAROOT"))
    except KeyError:
        pytest.skip(f"Schema folder not configured")
    version = f"{Version}"
    rdfs_path = find_rdfs_path(version)
    assert os.path.isdir(rdfs_path) and os.listdir(rdfs_path)


@pytest.mark.parametrize("Version", [(9), (153), ("foo"), ("ba")])
def test_find_invalid_rdfs_version(Version):
    try:
        os.path.isdir(get_path("SCHEMAROOT"))
    except KeyError:
        pytest.skip(f"Schema folder not configured")
    with pytest.raises((ValueError, NotImplementedError)) as ex_info:
        version = f"{Version}"
        find_rdfs_path(version)
    print(ex_info)
