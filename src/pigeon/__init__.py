"""
Pigeon, an OLAP engine for Oxford Nanopore data.

"""

from typing import Optional, Tuple
import pathlib as P
from urllib.parse import urlparse

import boto3
from botocore import UNSIGNED
from botocore.config import Config

import logging

# --------

log = logging.getLogger(__name__)


# --------

def make_unsigned_s3(session: Optional[boto3.Session]=None):
    """
    Create a boto3 session for making unsigned calls to S3.

    """
    if not session:
        session = boto3.Session()

    client = session.client('s3', config=Config(signature_version=UNSIGNED))

    return client


def split_bucket(url: str) -> Tuple[str, P.Path]:
    """
    Split an s3 URL into bucket and prefix, also normalise.

    """
    # Split path into bucket and key
    url = urlparse(url)
    if not url.scheme == 's3':
        raise ValueError("URL is not an S3 bucket")

    bucket = url.netloc

    # Remove '/' prefix
    prefix = P.Path(url.path[1:])

    return (bucket, prefix)
