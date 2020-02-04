
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

import intake  # Import this first to avoid circular imports during discovery.
del intake
from .spark_sources import SparkRDD, SparkDataFrame
from .spark_cat import SparkTablesCatalog
