from intake.source.base import DataSource, Schema
from .base import SparkHolder
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class SparkRDD(DataSource):
    container = 'python'
    version = __version__
    name = 'spark_rdd'
    partition_access = True

    def __init__(self, args, context_kwargs=None, metadata=None):
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
        self._get_schema()
        sc = self.holder.sc[0]
        return sc.runJob(self.ref, lambda x: x, partitions=[i])

    def to_spark(self):
        self._get_schema()
        return self.ref

    def to_dask(self):
        import dask.bag as db
        import dask
        self._get_schema()
        part = dask.delayed(self.read_partition)
        parts = [part(i) for i in range(self.npartitions)]
        return db.from_delayed(parts)

    def read(self):
        self._get_schema()
        return self.ref.collect()

    def _close(self):
        self.ref = None


class SparkDataFrame(DataSource):
    container = 'dataframe'
    version = __version__
    name = 'spark_dataframe'
    partition_access = True

    def __init__(self, args, context_kwargs=None, metadata=None):
        super(SparkDataFrame, self).__init__(metadata)
        self.holder = SparkHolder(True, args, context_kwargs)
        self.ref = None

    def _get_schema(self):
        if self.ref is None:
            self.ref = self.holder.setup()
            self.npartitions = self.ref.rdd.getNumPartitions()
            rows = self.ref.take(10)
            self.dtype = pandas_dtypes(self.ref.schema, rows)
            self.shape = (None,len(self.dtype))
        return Schema(npartitions=self.npartitions,
                      extra_metadata=self.metadata,
                      dtype=self.dtype,
                      shape=self.shape)

    def read_partition(self, i):
        import pandas as pd
        self._get_schema()
        sc = self.holder.sc[0]
        out = sc.runJob(self.ref.rdd, lambda x: x, partitions=[i])
        df = pd.DataFrame.from_records(out)
        df.columns = list(self.dtype)
        return df

    def to_spark(self):
        self._get_schema()
        return self.ref

    def read(self):
        self._get_schema()
        return self.ref.toPandas()

    def _close(self):
        self.ref = None


def pandas_dtypes(schema, rows):
    """Rough dtype for the given pyspark schema"""
    import pandas as pd
    from pyspark.sql.dataframe import (_to_corrected_pandas_type, IntegralType)
    # copied from toPandas() method
    df = pd.DataFrame.from_records(rows)
    df.columns = [s.name for s in schema]
    for field in schema:
        pandas_type = _to_corrected_pandas_type(field.dataType)
        if pandas_type is not None and not(
                isinstance(field.dataType, IntegralType) and field.nullable):
            df[field.name] = df[field.name].astype(pandas_type)
    return {k: str(v) for k, v in df.dtypes.to_dict().items()}
