from abc import ABC, abstractmethod
from typing import Optional
from urllib.parse import urlparse
import pathlib as P

import duckdb
import boto3

from . import split_bucket

SEQ_SCHEMAS = {
    'cramstats': [
        ('name', 'VARCHAR', 'YES', None, None, None),
        ('ref', 'VARCHAR', 'YES', None, None, None),
        ('coverage', 'DOUBLE', 'YES', None, None, None),
        ('ref_coverage', 'DOUBLE', 'YES', None, None, None),
        ('qstart', 'BIGINT', 'YES', None, None, None),
        ('qend', 'BIGINT', 'YES', None, None, None),
        ('rstart', 'BIGINT', 'YES', None, None, None),
        ('rend', 'BIGINT', 'YES', None, None, None),
        ('aligned_ref_len', 'BIGINT', 'YES', None, None, None),
        ('direction', 'VARCHAR', 'YES', None, None, None),
        ('length', 'BIGINT', 'YES', None, None, None),
        ('read_length', 'BIGINT', 'YES', None, None, None),
        ('match', 'BIGINT', 'YES', None, None, None),
        ('ins', 'BIGINT', 'YES', None, None, None),
        ('del', 'BIGINT', 'YES', None, None, None),
        ('sub', 'BIGINT', 'YES', None, None, None),
        ('iden', 'DOUBLE', 'YES', None, None, None),
        ('acc', 'DOUBLE', 'YES', None, None, None)
    ]
}


class CramStatsDir(ABC):
    """
    Interface to ways to extract CramStats table data from a filesystem.
    """

    @abstractmethod
    def make_table_relation(self, connection: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
        """
        Return a duckdb relation for the directory
        """
        raise NotImplementedError


class RemoteCramStatsDir(CramStatsDir):
    def __init__(self, url: str, s3_client: Optional['botocore.client.S3']=None):
        if not s3_client:
            s3_client = boto3.client('s3')
        self._s3 = s3_client

        self._bucket, self._prefix = split_bucket(url)

    def make_table_relation(self, connection: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
        csv_path = f's3://{self._bucket}/{self._prefix}'

        return connection.read_csv(csv_path)
