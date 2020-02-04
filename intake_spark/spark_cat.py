from intake.catalog.local import LocalCatalogEntry, Catalog
from .spark_sources import SparkDataFrame
from ._version import get_versions
from .base import SparkHolder


class SparkTablesCatalog(Catalog):
    """Intake automatically-generate catalog for tables stored in Spark

    This driver will query Spark's Catalog object for any tables, and
    create an entry for each which, when accessed, will instantiate
    SparkDataFrame sources. Commonly, these table definitions will come
    from Hive.
    """
    name = 'spark_cat'
    version = get_versions()['version']

    def __init__(self, database=None, context_kwargs=None, metadata=None):
        """
        Parameters
        ----------
        database: str or None
             If using a specific database, the name should be given here.
             If not given, will attempt to query all defined databases.
        context_kwargs: dict
            Passed to SparkHolder for establishing the context on which to
            communicate with Spark.
        """
        self.database = database
        self.context_args = context_kwargs
        self.spark_cat = None
        super(SparkTablesCatalog, self).__init__(metadata=metadata)

    def _load(self):
        if self.spark_cat is None:
            self.spark_cat = SparkHolder(True, [('catalog', )],
                                         self.context_args).setup()
        self._entries = {}
        dbs = (self.spark_cat.listDatabases()
               if self.database is None else [self.database])

        for db in dbs:
            tables = self.spark_cat.listTables(dbName=db.name)
            for table in tables:
                if db.name:
                    description = ('Spark table %s in database %s'
                                   '' % (table.name, db.name))
                else:
                    description = ('Spark table %s in default database'
                                   '' % table.name)
                args = {'args': [
                    ('table', (table.name, ))
                ]}
                e = LocalCatalogEntry(
                    table.name, description, 'spark_dataframe', True,
                    args, cache=[], parameters=[], metadata={}, catalog_dir="",
                    getenv=False, getshell=False)
                e._plugin = [SparkDataFrame]
                self._entries[table.name] = e
