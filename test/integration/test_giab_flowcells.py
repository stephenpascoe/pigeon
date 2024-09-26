"""
Test Pigeon interface to ONT's Genome in a Bottle dataset.

These tests interact with the ont-open-data bucket and are therefore integration tests.

"""

import pytest
from typing import Generator
import tempfile

import duckdb

import pigeon
from pigeon import RemoteFlowcellDir

bucket = 'ont-open-data'

eg_flowcell_run_id = 'c3641428eb90f0d05daec16022cd0cb46c20eafd'

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


@pytest.fixture
def ondisk_store() -> Generator[pigeon.Store, None, None]:
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.close()
        store = pigeon.Store(tmp.name)
        yield store
        store.close()


# --------
# RemoteFlowcellDir tests

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


@pytest.mark.parametrize('table_name', pigeon.SCHEMAS.keys())
def test_relation_columns(table_name: str, eg_flowcell_dir: RemoteFlowcellDir, store: pigeon.Store):
    """RemoteFlowcellDir can create a relation for each table with the correct columns"""

    # Store will add extra columns from funal_summary before insertion.
    # Therefore remove these from the columns to consider.
    columns = {x[0] for x in pigeon.SCHEMAS[table_name]}
    if table_name in ['pore_activity', 'throughput']:
        columns = columns ^ {'experiment_id', 'run_id'}

    # TODO : consider if Store._conn should be public
    rel1 = eg_flowcell_dir.make_table_relation(table_name, store._conn)

    assert type(rel1) is duckdb.DuckDBPyRelation
    assert set(rel1.columns) == columns


# --------
# Store tests


def test_store1(ondisk_store, eg_flowcell_dir):
    ondisk_store.insert_flowcell(eg_flowcell_dir)

    data = ondisk_store._conn.sql('select acquisition_run_id from final_summary').fetchall()
    
    assert len(data) == 1
    assert data[0][0] == eg_flowcell_run_id
