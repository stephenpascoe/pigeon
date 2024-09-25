"""
Test Pigeon interface to ONT's Genome in a Bottle dataset.

These tests interact with the ont-open-data bucket and are therefore integration tests.

"""

import pytest

import duckdb

import pigeon
from pigeon import RemoteFlowcellDir

bucket = 'ont-open-data'

# --------
# Fixtures

@pytest.fixture
def s3_client():
    s3 = pigeon.make_unsigned_s3()

    return s3

@pytest.fixture
def eg_flowcell_dir(s3_client) -> RemoteFlowcellDir:
    flowcell_prefix = 'giab_2023.05/flowcells/hg001/20230505_1857_1B_PAO99309_94e07fab/'

    fdir = RemoteFlowcellDir(f's3://{bucket}/{flowcell_prefix}', s3_client=s3_client)

    return fdir


@pytest.fixture
def store() -> pigeon.Store:
    return pigeon.Store(':memory:')



# --------
# Tests

def test_available_tables(eg_flowcell_dir: RemoteFlowcellDir):
    """The available table dictionary should have a final_summary table"""

    tables = eg_flowcell_dir.get_available_tables()

    assert 'final_summary' in tables


def test_table_name(eg_flowcell_dir: RemoteFlowcellDir):
    """The available table dictionary should have values of filenames of the tables"""
    tables = eg_flowcell_dir.get_available_tables()

    filename = str(tables['final_summary'])
    print(filename)

    assert filename.startswith('final_summary')
    assert filename.endswith('.txt')


def test_final_summary_relation(eg_flowcell_dir: RemoteFlowcellDir, store: pigeon.Store):
    """A flowcell_dir can return a relation which extracts final-summary data"""

    columns = [x[0] for x in pigeon.SCHEMAS['final_summary']]

    # TODO : consider if Store._conn should be public
    rel1 = eg_flowcell_dir.make_table_relation('final_summary', store._conn)

    assert rel1.columns == columns
