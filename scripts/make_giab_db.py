#!/usr/bin/env python
"""
Create a duckdb database for data in the ONT Genome in a bottle (GIAB) dataset.

"""

import sys
import logging

import pigeon.flowcell_dir
import pigeon.store
import pigeon.cramstats_dir

bucket = 'ont-open-data'
flowcell_path = 'giab_2023.05/flowcells/'
cramstats_path = 'giab_2023.05/analysis/stats'

log = logging.getLogger('make_giab_db')


def get_flowcell_paths(s3_client):
    genome_prefixes = [x['Prefix'] for x in s3_client.list_objects(Bucket=bucket, Prefix=flowcell_path, Delimiter='/')['CommonPrefixes']]

    for prefix in genome_prefixes:
        for x in s3_client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')['CommonPrefixes']:
            yield x['Prefix']


def cramstat_paths(s3_client):
    for x in s3_client.list_objects(Bucket=bucket, Prefix=cramstats_path):
        if x.endswith('cram.stats'):
            yield x


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # TODO : better argument handling
    (db_path, ) = sys.argv[1:]

    s3_client = pigeon.make_unsigned_s3()
    store = pigeon.store.Store(db_path)

    for path in get_flowcell_paths(s3_client):
        log.info(f'Processing {path}')
        fdir = pigeon.flowcell_dir.RemoteFlowcellDir(f's3://{bucket}/{path}', s3_client)
        store.insert_flowcell(fdir)

    for path in cramstat_paths(s3_client):
        log.info(f'Processing {path}')
        cdir = pigeon.cramstats_dir.RemoteCramStatsDir(f's3://{bucket}/{path}', s3_client)
        store.insert_cramstats(cdir)

    store.close()
