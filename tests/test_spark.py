import os
import pandas as pd
from intake_spark import SparkDataFrame, SparkRDD

TEST_DATA_DIR = 'tests'
TEST_DATA = 'sample1.csv'
fn = os.path.join(TEST_DATA_DIR, TEST_DATA)
df = pd.read_csv(fn)


def test_rdd():
    text = SparkRDD(
        [('textFile', (fn,)),
         ('map', (len,))])
    expected = [len(l) - 1 for l in open(fn)]  # spark trims newlines
    assert text.read() == expected


def test_df():
    text = SparkDataFrame([
        ('read', ),
        ('format', ('csv', )),
        ('option', ('header', 'true')),
        ('load', (fn, ))
    ], {})
    d = text.read()
    assert d.astype(df.dtypes).equals(df)
