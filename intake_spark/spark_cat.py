from intake.catalog.local import LocalCatalogEntry, Catalog
from . import SparkDataFrame
from .base import SparkHolder


class SparkTablesCatalog(Catalog):

    def __init__(self, database=None, context_kwargs=None, metadata=None):
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
                description = 'Spark table %s in database %s' % (
                    table.name, db.name)
                args = {'args': [
                    ('table', (table.name, ))
                ]}
                e = LocalCatalogEntry(
                    table.name, description, 'spark_dataframe', True,
                    args, {}, {}, {}, "", getenv=False, getshell=False)
                e._plugin = [SparkDataFrame]
                self._entries[table.name] = e
