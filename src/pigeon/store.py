import itertools
import logging

import duckdb

from pigeon import SEQ_SCHEMAS
from pigeon.flowcell_dir import FC_SCHEMAS, FlowcellDir

log = logging.getLogger(__name__)

class Store:
    """
    A store is your one-stop-shop for querying signal, reads and alignments.

    """

    def __init__(self, path: str):
        """
        :param path: path to underlying duckdb database
        :param boto_session: Session to use for connecting to S3

        """
        self._conn = duckdb.connect(path)
        if not self._has_schema():
            self._init_schema()

    def close(self):
        self._conn.close()

    # --------

    def _has_schema(self):
        tables = [x[0] for x in self._conn.sql('show tables').fetchall()]
        return 'final_summary' in tables

    def _init_schema(self):
        for table_name, schema in itertools.chain(FC_SCHEMAS.items(), SEQ_SCHEMAS.items()):
            col_expr = []
            for col in schema:
                # TODO : Add nullable option
                col_expr.append(f'{col[0]} {col[1]}')
            col_expr_str = ', '.join(col_expr)
            sql = f'create or replace table {table_name} ({col_expr_str})'
            log.info(f'Creating table {table_name}')
            log.debug(sql)
            self._conn.sql(sql)

    def insert_flowcell(self, flowcell_dir: FlowcellDir) -> None:
        rel = flowcell_dir.make_table_relation('final_summary', self._conn)

        final_summary = {k: v for (k, v) in zip(rel.columns, rel.fetchone())}

        # TODO : Resolve run_id vs acquisition_run_id
        run_id = final_summary['acquisition_run_id']
        log.info(f'Inserting flowcell run {run_id}')

        log.info(f'Inserting final_summary for {run_id}')
        self._conn.execute('insert into final_summary by name (select * from rel)')

        log.info(f'Inserting pore_activity for {run_id}')
        rel = flowcell_dir.make_table_relation('pore_activity', self._conn)
        # Join experiment_id and run_id
        rel = rel.project(f"""
                '{final_summary['protocol_group_id']}' as experiment_id, 
                '{final_summary['acquisition_run_id']}' as run_id, *
                """)
        self._conn.execute('insert into pore_activity by name (select * from rel)')

        log.info(f'Inserting throughput for {run_id}')
        rel = flowcell_dir.make_table_relation('throughput', self._conn)
        # Join experiment_id and run_id
        rel = rel.project(f"""
                '{final_summary['protocol_group_id']}' as experiment_id, 
                '{final_summary['acquisition_run_id']}' as run_id, *
                """)
        self._conn.execute('insert into throughput by name (select * from rel)')

        log.info(f'Inserting sequencing_summary for {run_id}')
        rel = flowcell_dir.make_table_relation('sequencing_summary', self._conn)
        self._conn.execute('insert into sequencing_summary by name (select * from rel)')
