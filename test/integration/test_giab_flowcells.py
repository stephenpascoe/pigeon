"""
Test Pigeon interface to ONT's Genome in a Bottle dataset.

These tests interact with the ont-open-data bucket and are therefore integration tests.

"""

import pytest
from typing import Generator
import tempfile

import duckdb

import pigeon
import pigeon.flowcell_dir
import pigeon.store
from pigeon.flowcell_dir import RemoteFlowcellDir
from pigeon.cramstats_dir import RemoteCramStatsDir

bucket = 'ont-open-data'

eg_flowcell_run_id = 'c3641428eb90f0d05daec16022cd0cb46c20eafd'
eg_flowcell_experiment_id = 'r10p41_e8p2_human_runs_jkw'

# --------
# Fixtures

class TruncatedRemoteFlowcellDir(RemoteFlowcellDir):
    """Overrides relations returned from a RemoteFlowcellDir to reduce the number of rows returned"""
    def make_table_relation(self, table_name: str, connection: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
        rel = super().make_table_relation(table_name, connection)

        return rel.limit(50)


class TruncatedRemoteCramstatsDir(RemoteCramStatsDir):
    def make_table_relation(self, connection: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
        rel = super().make_table_relation(connection)

        return rel.limit(50)


@pytest.fixture(scope='module')
def s3_client():
    s3 = pigeon.make_unsigned_s3()

    return s3

@pytest.fixture(scope='module')
def eg_flowcell_dir(s3_client) -> RemoteFlowcellDir:
    flowcell_prefix = 'giab_2023.05/flowcells/hg001/20230505_1857_1B_PAO99309_94e07fab/'

    fdir = TruncatedRemoteFlowcellDir(f's3://{bucket}/{flowcell_prefix}', s3_client=s3_client)

    return fdir

@pytest.fixture(scope='module')
def eg_cramstats_dir(s3_client) -> RemoteCramStatsDir:
    cramstats_prefix = 'giab_2023.05/analysis/stats/hac_PAO83395.cram.stats'

    cdir = TruncatedRemoteCramstatsDir(f's3://{bucket}/{cramstats_prefix}', s3_client=s3_client)

    return cdir


@pytest.fixture
def store() -> pigeon.store.Store:
    return pigeon.store.Store(':memory:')


@pytest.fixture(scope='module')
def ondisk_store() -> Generator[pigeon.store.Store, None, None]:
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.close()
        store = pigeon.store.Store(tmp.name)
        yield store
        store.close()


@pytest.fixture(scope='module')
def store_with_flowcell(eg_flowcell_dir, ondisk_store) -> pigeon.store.Store:
    ondisk_store.insert_flowcell(eg_flowcell_dir)
    return ondisk_store

@pytest.fixture(scope='module')
def store_with_cramstats(eg_cramstats_dir, ondisk_store) -> pigeon.store.Store:
    ondisk_store.insert_cramstats(eg_cramstats_dir)
    return ondisk_store

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


@pytest.mark.parametrize('table_name', pigeon.flowcell_dir.FC_SCHEMAS.keys())
def test_relation_columns(table_name: str, eg_flowcell_dir: RemoteFlowcellDir, store: pigeon.store.Store):
    """RemoteFlowcellDir can create a relation for each table with the correct columns"""

    # Store will add extra columns from funal_summary before insertion.
    # Therefore remove these from the columns to consider.
    columns = {x[0] for x in pigeon.flowcell_dir.FC_SCHEMAS[table_name]}
    if table_name in ['pore_activity', 'throughput']:
        columns = columns ^ {'experiment_id', 'run_id'}

    # TODO : consider if Store._conn should be public
    rel1 = eg_flowcell_dir.make_table_relation(table_name, store._conn)

    assert type(rel1) is duckdb.DuckDBPyRelation
    assert set(rel1.columns) == columns


# --------
# Store tests


def test_store1(store_with_flowcell):
    data = store_with_flowcell._conn.sql('select acquisition_run_id from final_summary').fetchall()
    
    assert len(data) == 1
    assert data[0][0] == eg_flowcell_run_id


def test_store2(store_with_flowcell):
    rel = store_with_flowcell._conn.sql('select * from pore_activity')
    row = rel.fetchone()
    row_dict = {k: v for (k, v) in zip(rel.columns, row)}

    print(row)

    assert row_dict['run_id'] == eg_flowcell_run_id
    assert row_dict['experiment_id'] == eg_flowcell_experiment_id


def test_store3(store_with_flowcell):
    rel = store_with_flowcell._conn.sql('select * from throughput')
    row = rel.fetchone()
    row_dict = {k: v for (k, v) in zip(rel.columns, row)}

    print(row)

    assert row_dict['run_id'] == eg_flowcell_run_id
    assert row_dict['experiment_id'] == eg_flowcell_experiment_id


def test_store4(store_with_flowcell):
    rel = store_with_flowcell._conn.sql('select * from sequencing_summary')
    row = rel.fetchone()
    row_dict = {k: v for (k, v) in zip(rel.columns, row)}

    print(row)

    assert row_dict['run_id'] == eg_flowcell_run_id
    assert row_dict['experiment_id'] == eg_flowcell_experiment_id


def test_store_cramstats(store):
    """Verify the cramstats table is created"""
    rel = store._conn.sql('show tables')
    tables = [x[0] for x in rel.fetchall()]

    assert 'cramstats' in tables

def test_cramstats_relation(store_with_cramstats):
    rel = store_with_cramstats._conn.sql('select * from cramstats')
    row = rel.fetchone()
    row_dict = {k: v for (k, v) in zip(rel.columns, row)}

    print(row_dict)

    assert row_dict['ref'] == 'chr1'
