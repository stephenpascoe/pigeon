#!/usr/bin/env python
"""
Create a duckdb database for data in the ONT Genome in a bottle (GIAB) dataset.

"""

import sys
import logging

import pigeon


bucket = 'ont-open-data'
flowcell_path = 'giab_2023.05/flowcells/'

log = logging.getLogger('make_giab_db')


def get_flowcell_paths(s3_client):
    genome_prefixes = [x['Prefix'] for x in s3_client.list_objects(Bucket=bucket, Prefix=flowcell_path, Delimiter='/')['CommonPrefixes']]

    for prefix in genome_prefixes:
        for x in s3_client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')['CommonPrefixes']:
            yield x['Prefix']



if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # TODO : better argument handling
    (db_path, ) = sys.argv[1:]

    s3_client = pigeon.make_unsigned_s3()
    store = pigeon.Store(db_path)

    for path in get_flowcell_paths(s3_client):
        log.info(f'Processing {path}')
        fdir = pigeon.RemoteFlowcellDir(f's3://{bucket}/{path}', s3_client)
        store.insert_flowcell(fdir)

    store.close()
