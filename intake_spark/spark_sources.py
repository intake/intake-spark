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

    def __init__(self, method, context_kwargs, *args, **kwargs):
        metadata = kwargs.pop('metadata', {})
        super(SparkRDD, self).__init__(metadata)
        self.holder = SparkHolder(False, method, context_kwargs,
                                  *args, **kwargs)
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
