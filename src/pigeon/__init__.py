"""
Pigeon, an OLAP engine for Oxford Nanopore data.

"""

from typing import Optional

import boto3
from botocore import UNSIGNED
from botocore.config import Config

import logging

# --------

log = logging.getLogger(__name__)


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

# --------

def make_unsigned_s3(session: Optional[boto3.Session]=None):
    """
    Create a boto3 session for making unsigned calls to S3.

    """
    if not session:
        session = boto3.Session()

    client = session.client('s3', config=Config(signature_version=UNSIGNED))

    return client
