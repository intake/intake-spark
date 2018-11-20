import os
import pandas as pd
from intake_spark import SparkDataFrame, SparkRDD, SparkTablesCatalog
from intake_spark.base import SparkHolder
import intake

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


def test_readtext():
    source = intake.open_textfiles(fn)
    rdd = source.to_spark()
    out = rdd.collect()
    assert '\n'.join(out) == open(fn).read().rstrip('\n')


def test_df():
    text = SparkDataFrame([
        ('read', ),
        ('format', ('csv', )),
        ('option', ('header', 'true')),
        ('load', (fn, ))
    ], {})
    d = text.read()
    assert d.astype(df.dtypes).equals(df)


def test_cat():
    import pyspark
    h = SparkHolder(True, [('catalog', )], {})
    h.setup()  # create spark session early
    session = h.session[0]
    d = session.createDataFrame(df)
    sql = pyspark.HiveContext(session.sparkContext)
    sql.registerDataFrameAsTable(d, 'temp')

    cat = SparkTablesCatalog()
    assert 'temp' in list(cat)
    s = cat.temp()
    assert isinstance(s, SparkDataFrame)
    out = s.read()
    assert out.astype(df.dtypes).equals(df)
