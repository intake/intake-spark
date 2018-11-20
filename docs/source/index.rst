Welcome to intake-spark's documentation!
========================================

This package enables the Intake data access and catalog system to read from Apache Spark.

This package provides Spark RDD and DataFrame drivers, and passes chained set of arguments on to pyspark,
and there is also a driver to view the Spark catalog (often with tables provided by Hive) as Intake entries.

Be sure to read the :ref:`quickstart` on how to provide the context/parameters required by Spark and how to
phrase a Spark loading invocation for Intake.

This package is required by Intake for use of any ``.to_spark()`` methods.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   quickstart.rst
   api.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
