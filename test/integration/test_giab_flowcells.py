"""
Test Pigeon interface to ONT's Genome in a Bottle dataset.

These tests interact with the ont-open-data bucket and are therefore integration tests.

"""

from . import pigeon

bucket = 'ont-open-data'

def test_available_tables():
    flowcell_prefix = 'giab_2023.05/flowcells/hg001/20230505_1857_1B_PAO99309_94e07fab/'

    # TODO : make a fixture
    s3 = pigeon.make_unsigned_s3()

    fdir = pigeon.RemoteFlowcellDir(f's3://{bucket}/{flowcell_prefix}')

    tables = fdir.get_available_tables()

    assert 'final_summary' in tables