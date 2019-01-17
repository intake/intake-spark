Quickstart
==========

``intake-spark`` provides quick and easy access to data via
`Apache Spark`_

.. _Apache Spark: http://spark.apache.org/docs/latest/


Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c conda-forge intake-spark

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Establishing a Context
~~~~~~~~~~~~~~~~~~~~~~

Operations on a Spark cluster are achieved via a "context", which is a python-side client to the remote system. All
use of this package require a valid context in order to work. There are two ways to establish the context:

- use the function ``intake_spark.base.SparkHolder.set_class_session()``, passing for ``context=`` and ``session=``
  the respective existing objects that have been created previously using conventional means. In this case, Spark
  connection-specific parameters are not encoded in the catalog. Since intake-spark uses ``getOrCreate()``, the
  existence of the objects should be enough for them to be picked up at data access time, but we still recommend
  calling the set function explicitly. Note that if only using RDDs and no SQl functionality, the ``session`` need
  not be created or provided.

- the same function can also take a set of parameters, in which case intake-spark will attempt to create the context
  and session for you, passing the parameters (master, app_name, executor_env, spark_home and config parameters) on
  to Spark. In this case, the parameters can be stored in a catalog (the ``context_kwargs`` parameter, a dictionary).
  If providing an empty set of parameters, Spark will create the default local context, which is useful only for
  testing.

.. code-block:: python

   # in-code example
   from intake_spark.base import SparkHolder
   import intake
   SparkHolder.set_class_session(master='spark://myhost:7077', app_name='intake', hive=True)

Encoding Spark calls
~~~~~~~~~~~~~~~~~~~~

Spark calls are often expressed as a chain of attribute look-ups with some being called as methods with arguments.
To encode these for Intake, we make each stage of the chain an element in a list, starting from the Context or
Session for the RDD and DataFrame versions of the driver. For example, the following encodes
``sc.textfile('s3://bucket/files*.txt')``, i.e., a single element of attribute lookup

.. code-block:: python

   source = intake.open_spark_rdd([
       ['textFile', ['s3://bucket/files*.txt',]],
       ['map', [len, ]]
       ])
   rdd = source.to_spark()

Here ``rdd`` will be a pySpark RDD instance.

A more complicated example, for encoding ``spark.read.option("mergeSchema", "true").parquet("data/test_table")``
for use with Intake

.. code-block:: python

   source = intake.open_spark_dataframe([
       ['read', ],
       ['option', ["mergeSchema", "true"]],
       ['parquet', ["data/test_table",]]
       ])
   df = source.to_spark()

Note that you *can* pass functions using this formalism, but only encode python built-ins into a YAML file,
e.g., ``len => !!python/name:builtins.len ''``.

Using in a catalog
~~~~~~~~~~~~~~~~~~

The above example could be expressed in YAML syntax as follows

.. code-block:: yaml

    sources:
      spark_dataframe:
        args:
          args:
          - - read
          - - option
            - - mergeSchema
              - 'true'
          - - parquet
            - - data/test_table
          context_kwargs:
            master: "spark://myhost:7077"
            app_name: intake
        description: ''
        driver: spark_dataframe
        metadata: {}

Note the complex nesting pattern, and that in this case we are including arguments for creating an appropriate
SparkContext and Session on the fly, if one has not already been made.
