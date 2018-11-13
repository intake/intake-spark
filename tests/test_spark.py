import os
import pickle
import time

import pytest
import pandas as pd

from .util import verify_datasource_interface

CONNECT = {'host': 'localhost', 'port': 9200}
TEST_DATA_DIR = 'tests'
TEST_DATA = 'sample1.csv'
df = pd.read_csv(os.path.join(TEST_DATA_DIR, TEST_DATA))


@pytest.fixture(scope='module')
def spark():
    """Start docker container for ES and cleanup connection afterward."""
    import pyspark

    try:
        sc = pyspark.SparkContext.getOrCreate()
        yield dict(sc.getConf().getAll())
    finally:
        sc.stop()


