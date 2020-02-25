from intake.source.base import DataSource, Schema
from .base import SparkHolder
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class SparkRDD(DataSource):
    """A reference to an RDD definition in Spark

    RDDs are list-of-things objects, evaluated lazily in Spark.

    Examples
    --------
    >>> args = [('textFile', ('text.*.files', )),
    ...         ('map', (len,))]
    >>> context = {'master': 'spark://master.node:7077'}
    >>> source = SparkRDD(args, context)

    The output of `source.to_spark()` is an RDD object holding the lengths of
    the lines of the input files.
    """
    container = 'python'
    version = __version__
    name = 'spark_rdd'
    partition_access = True

    def __init__(self, args, context_kwargs=None, metadata=None):
        """
        Parameters
        ----------

        args, context_kwargs:
            Passed on to SparkHolder, see its docstrings and the examples.
        metadata: dict
            Arbitrary data to associate with this source.
        """
        super(SparkRDD, self).__init__(metadata)
        self.holder = SparkHolder(False, args, context_kwargs)
        self.ref = None

    def _get_schema(self):
        if self.ref is None:
            self.ref = self.holder.setup()
            self.npartitions = self.ref.getNumPartitions()
        return Schema(npartitions=self.npartitions,
                      extra_metadata=self.metadata)

    def read_partition(self, i):
        """Returns one of the partitions of the RDD as a list of objects"""
        self._get_schema()
        sc = self.holder.sc[0]
        return sc.runJob(self.ref, lambda x: x, partitions=[i])

    def to_spark(self):
        """Return the spark object for this data, an RDD"""
        self._get_schema()
        return self.ref

    def read(self):
        """Materialise the whole RDD into a list of objects"""
        self._get_schema()
        return self.ref.collect()

    def _close(self):
        self.ref = None


class SparkDataFrame(DataSource):
    """A reference to a DataFrame definition in Spark

    DataFrames are tabular spark objects containing a heterogeneous set of
    columns and potentially a large number of rows. They are similar in concept
    to Pandas or Dask data-frames. The Spark variety produced by this driver
    will be a handle to a lazy object, where computation will be managed by
    Spark.

    Examples
    --------
    >>> args = [
    ...    ['read', ],
    ...    ['format', ['csv', ]],
    ...    ['option', ['header', 'true']],
    ...    ['load', ['data.*.csv', ]]
    ...    ]
    >>> context = {'master': 'spark://master.node:7077'}
    >>> source = SparkDataFrame(args, context)

    The output of `source.to_spark()` contains a spark object pointing to the
    parsed contents of the indicated CSV files
    """
    container = 'dataframe'
    version = __version__
    name = 'spark_dataframe'
    partition_access = True

    def __init__(self, args, context_kwargs=None, metadata=None):
        """
        Parameters
        ----------

        args, context_kwargs:
            Passed on to SparkHolder, see its docstrings and the examples.
        metadata: dict
            Arbitrary data to associate with this source.
        """
        super(SparkDataFrame, self).__init__(metadata)
        self.holder = SparkHolder(True, args, context_kwargs)
        self.ref = None

    def _get_schema(self):
        if self.ref is None:
            self.ref = self.holder.setup()
            self.npartitions = self.ref.rdd.getNumPartitions()
            rows = self.ref.take(10)
            self.dtype = pandas_dtypes(self.ref.schema, rows)
            self.shape = (None, len(self.dtype))
        return Schema(npartitions=self.npartitions,
                      extra_metadata=self.metadata,
                      dtype=self.dtype,
                      shape=self.shape)

    def read_partition(self, i):
        """Returns one partition of the data as a pandas data-frame"""
        import pandas as pd
        self._get_schema()
        sc = self.holder.sc[0]
        out = sc.runJob(self.ref.rdd, lambda x: x, partitions=[i])
        df = pd.DataFrame.from_records(out)
        df.columns = list(self.dtype)
        return df

    def to_spark(self):
        """Return the Spark object for this data, a DataFrame"""
        self._get_schema()
        return self.ref

    def read(self):
        """Read all of the data into an in-memory Pandas data-frame"""
        self._get_schema()
        return self.ref.toPandas()

    def _close(self):
        self.ref = None


def _to_corrected_pandas_type(dt):
    # Copied from pyspark 2.3
    """
    When converting Spark SQL records to Pandas DataFrame,
    the inferred data type may be wrong. This method gets the corrected
    data type for Pandas if that type may be inferred uncorrectly.
    """
    import numpy as np
    from pyspark.sql.types import ByteType, ShortType, IntegerType, FloatType
    if type(dt) == ByteType:
        return np.int8
    elif type(dt) == ShortType:
        return np.int16
    elif type(dt) == IntegerType:
        return np.int32
    elif type(dt) == FloatType:
        return np.float32
    else:
        return None


def pandas_dtypes(schema, rows):
    """Rough dtype for the given pyspark schema"""
    import pandas as pd
    from pyspark.sql.types import IntegralType
    # copied from toPandas() method
    df = pd.DataFrame.from_records(rows)
    df.columns = [s.name for s in schema]
    for field in schema:
        pandas_type = _to_corrected_pandas_type(field.dataType)
        if pandas_type is not None and not(
                isinstance(field.dataType, IntegralType) and field.nullable):
            df[field.name] = df[field.name].astype(pandas_type)
    return {k: str(v) for k, v in df.dtypes.to_dict().items()}
