"""
Test Pigeon interface to ONT's Genome in a Bottle dataset.

These tests interact with the ont-open-data bucket and are therefore integration tests.

"""

import pigeon
import pytest


bucket = 'ont-open-data'

# --------
# Fixtures

@pytest.fixture
def s3_client():
    s3 = pigeon.make_unsigned_s3()

    return s3

@pytest.fixture
def eg_flowcell_dir(s3_client):
    flowcell_prefix = 'giab_2023.05/flowcells/hg001/20230505_1857_1B_PAO99309_94e07fab/'

    fdir = pigeon.RemoteFlowcellDir(f's3://{bucket}/{flowcell_prefix}', s3_client=s3_client)

    return fdir

# --------
# Tests

def test_available_tables(eg_flowcell_dir):

    tables = eg_flowcell_dir.get_available_tables()

    assert 'final_summary' in tables


def test_table_name(eg_flowcell_dir):
    """The available table dictionary should have values of filenames of the tables"""
    tables = eg_flowcell_dir.get_available_tables()

    filename = tables['final_summary']
    print(filename)

    assert filename.startswith('final_summary')
    assert filename.endswith('.txt')
