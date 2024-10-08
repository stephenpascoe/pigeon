import pathlib as P
import re
from abc import ABC, abstractmethod
from typing import Dict, Optional
from urllib.parse import urlparse
import logging

import boto3
import duckdb

from . import split_bucket

log = logging.getLogger(__name__)


# These were extracted from duckdb's schema auto-detection

FC_SCHEMAS = {
        'final_summary': [
            ('acquisition_run_id', 'VARCHAR', 'YES', None, None, None),
            ('acquisition_stopped', 'VARCHAR', 'YES', None, None, None),
            ('basecalling_enabled', 'VARCHAR', 'YES', None, None, None),
            ('fast5_files_in_fallback', 'VARCHAR', 'YES', None, None, None),
            ('fast5_files_in_final_dest', 'VARCHAR', 'YES', None, None, None),
            ('fastq_files_in_fallback', 'VARCHAR', 'YES', None, None, None),
            ('fastq_files_in_final_dest', 'VARCHAR', 'YES', None, None, None),
            ('flow_cell_id', 'VARCHAR', 'YES', None, None, None),
            ('instrument', 'VARCHAR', 'YES', None, None, None),
            ('position', 'VARCHAR', 'YES', None, None, None),
            ('processing_stopped', 'VARCHAR', 'YES', None, None, None),
            ('protocol', 'VARCHAR', 'YES', None, None, None),
            ('protocol_group_id', 'VARCHAR', 'YES', None, None, None),
            ('protocol_run_id', 'VARCHAR', 'YES', None, None, None),
            ('sample_id', 'VARCHAR', 'YES', None, None, None),
            ('sequencing_summary_file', 'VARCHAR', 'YES', None, None, None),
            ('started', 'VARCHAR', 'YES', None, None, None)
        ],
        'pore_activity': [
            ('experiment_id', 'VARCHAR', 'YES', None, None, None),
            ('run_id', 'VARCHAR', 'YES', None, None, None),
            ('experiment_time', 'BIGINT', 'YES', None, None, None),
            ('adapter', 'HUGEINT', 'YES', None, None, None),
            ('disabled', 'HUGEINT', 'YES', None, None, None),
            ('locked', 'HUGEINT', 'YES', None, None, None),
            ('multiple', 'HUGEINT', 'YES', None, None, None),
            ('no_pore', 'HUGEINT', 'YES', None, None, None),
            ('pending_manual_reset', 'HUGEINT', 'YES', None, None, None),
            ('pending_mux_change', 'HUGEINT', 'YES', None, None, None),
            ('pore', 'HUGEINT', 'YES', None, None, None),
            ('saturated', 'HUGEINT', 'YES', None, None, None),
            ('strand', 'HUGEINT', 'YES', None, None, None),
            ('unavailable', 'HUGEINT', 'YES', None, None, None),
            ('unblocking', 'HUGEINT', 'YES', None, None, None),
            ('unclassified', 'HUGEINT', 'YES', None, None, None),
            ('unclassified_following_reset', 'HUGEINT', 'YES', None, None, None),
            ('unknown_negative', 'HUGEINT', 'YES', None, None, None),
            ('unknown_positive', 'HUGEINT', 'YES', None, None, None),
            ('zero', 'HUGEINT', 'YES', None, None, None)
        ],
        'throughput': [
            ('experiment_id', 'VARCHAR', 'YES', None, None, None),
            ('run_id', 'VARCHAR', 'YES', None, None, None),
            ('experiment_time', 'BIGINT', 'YES', None, None, None),
            ('reads', 'BIGINT', 'YES', None, None, None),
            ('basecalled_reads_passed', 'BIGINT', 'YES', None, None, None),
            ('basecalled_reads_failed', 'BIGINT', 'YES', None, None, None),
            ('basecalled_reads_skipped', 'BIGINT', 'YES', None, None, None),
            ('selected_raw_samples', 'BIGINT', 'YES', None, None, None),
            ('selected_events', 'BIGINT', 'YES', None, None, None),
            ('estimated_bases', 'BIGINT', 'YES', None, None, None),
            ('basecalled_bases', 'BIGINT', 'YES', None, None, None),
            ('basecalled_samples', 'BIGINT', 'YES', None, None, None)
        ],
        'sequencing_summary': [
            ('filename_fastq', 'VARCHAR', 'YES', None, None, None),
            ('filename_fast5', 'VARCHAR', 'YES', None, None, None),
            ('filename_pod5', 'VARCHAR', 'YES', None, None, None),
            ('parent_read_id', 'VARCHAR', 'YES', None, None, None),
            ('read_id', 'VARCHAR', 'YES', None, None, None),
            ('run_id', 'VARCHAR', 'YES', None, None, None),
            ('channel', 'BIGINT', 'YES', None, None, None),
            ('mux', 'BIGINT', 'YES', None, None, None),
            ('minknow_events', 'BIGINT', 'YES', None, None, None),
            ('start_time', 'DOUBLE', 'YES', None, None, None),
            ('duration', 'DOUBLE', 'YES', None, None, None),
            ('passes_filtering', 'BOOLEAN', 'YES', None, None, None),
            ('template_start', 'DOUBLE', 'YES', None, None, None),
            ('num_events_template', 'BIGINT', 'YES', None, None, None),
            ('template_duration', 'DOUBLE', 'YES', None, None, None),
            ('sequence_length_template', 'BIGINT', 'YES', None, None, None),
            ('mean_qscore_template', 'DOUBLE', 'YES', None, None, None),
            ('strand_score_template', 'DOUBLE', 'YES', None, None, None),
            ('median_template', 'DOUBLE', 'YES', None, None, None),
            ('mad_template', 'DOUBLE', 'YES', None, None, None),
            ('pore_type', 'VARCHAR', 'YES', None, None, None),
            ('experiment_id', 'VARCHAR', 'YES', None, None, None),
            ('sample_id', 'VARCHAR', 'YES', None, None, None),
            ('end_reason', 'VARCHAR', 'YES', None, None, None)
        ]
    }

class FlowcellDirError(Exception):
    pass

class TableNotPresent(FlowcellDirError):
    pass

class FlowcellDir(ABC):
    """Interface to ways to extract flowcell table data from a filesystem of some sort.
    """

    @abstractmethod
    def get_available_tables(self) -> Dict[str, P.Path]:
        """
        Return a dictionary of table files available in this flowcell directory.
        """
        raise NotImplementedError

    @abstractmethod
    def make_table_relation(self, table_name: str, connection: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
        """
        Return a duckdb relation for a given table
        """
        raise NotImplementedError


class RemoteFlowcellDir(FlowcellDir):
    def __init__(self, url: str, s3_client: Optional['botocore.client.S3']=None):
        if not s3_client:
            s3_client = boto3.client('s3')
        self._s3 = s3_client

        self._bucket, self._prefix = split_bucket(url)

    def get_available_tables(self) -> Dict[str, P.Path]:
        tables = {}

        resp = self._s3.list_objects(
            Bucket=self._bucket, Prefix=self._prefix.as_posix()+'/', Delimiter='/'
        )
        if 'Contents' in resp:
            for path in (x['Key'] for x in resp['Contents']):
                if table_name := self._table_name_from_path(path):
                    tables[table_name] = P.Path(path).relative_to(self._prefix)

        return tables

    def make_table_relation(self, table_name: str, connection: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
        tables = self.get_available_tables()
        if table_name not in tables:
            raise TableNotPresent(f"Table {table_name} not present for this flowcell")

        csv_path = f's3://{self._bucket}/{self._prefix}/{tables[table_name]}'

        match table_name:
            case 'final_summary':
                rel = connection.sql(f"pivot read_csv('{csv_path}', header=false, delim='=', names=['key', 'value']) on key using any_value(value)")
            case 'pore_activity':
                rel = connection.sql(f"""
                    pivot read_csv('{csv_path}', 
                                names=['channel_state', 'experiment_time', 'state_time']
                            )
                    on channel_state
                    using sum(state_time)
                    group by experiment_time
                    order by experiment_time
                """)
            case 'throughput':
                rel = connection.read_csv(csv_path,
                        names=['experiment_time', 'reads', 'basecalled_reads_passed', 'basecalled_reads_failed', 'basecalled_reads_skipped', 'selected_raw_samples',
                        'selected_events', 'estimated_bases', 'basecalled_bases',  'basecalled_samples']
                )
            case 'sequencing_summary':
                rel = connection.read_csv(csv_path)
            case _:
                raise ValueError(f'unhandled table name {table_name}')

        return rel

    # --------

    @staticmethod
    def _table_name_from_path(path: str) -> Optional[str]:
        p = P.Path(path)
        if p.suffix not in ['.tsv', '.txt', '.csv']:
            return None

        # Detect flowcell-id
        flowcell_id = p.parts[-2].split('_')[-2]

        # Get table name from filename before flowcell_id
        if table_name := re.search(f'(.*)_{flowcell_id}_.*', p.name):
            return table_name.group(1)
        else:
            # TODO : GIAB dataset contains extra tables: "full_ss_every_17.txt"
            log.warning(f'Potential table file not recognised {p}')
            return None
