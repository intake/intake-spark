

class SparkHolder(object):
    sc = [None]
    session = [None]

    def set_context(self, sc=None, conf=None, master=None, app_name=None,
                    executor_env=None, spark_home=None):
        """Establish spark context for this session

        Parameters
        ----------
        sc: SparkContext instance or None
            If given, this is the context that will be sued and all other
            parameters are ignored
        conf: dict or None
            If given, the key/values of the SparkConf to set. Some values
            may be over-written by the other kwargs given
        master: str or None
            The connection to establish
        app_name: str or None
            Identifies this usage
        executor_env: dict or None
            If given, environment variables values passed on
        spark_home: str
            Location of spark installation
        """
        import pyspark
        if sc is None:
            config = pyspark.SparkConf()
            config.setAll((conf or self.context_kwargs).items())
            if master is not None:
                config.setMaster(master)
            if app_name is not None:
                config.setAppName(app_name)
            if executor_env is not None:
                config.setExecutorEnv(pairs=list(executor_env.items()))
            if spark_home is not None:
                config.setSparkHome(spark_home)
            sc = pyspark.SparkContext.getOrCreate(config)
        self.sc[0] = sc

    @classmethod
    def set_session(cls, session=None, hive=True):
        """Set global SQL SparkSession"""
        if session is None:
            from pyspark import sql
            if hive:
                session = sql.SparkSession.builder.enableHiveSupport(
                    ).getOrCreate()
            else:
                session = sql.SparkSession.enableHiveSupport().getOrCreate()
        cls.session[0] = session

    def __init__(self, sql, method, context_kwargs, *args, **kwargs):
        """Create reference to spark resource

        Parameters
        ----------
        sql: bool
            If True, will use SQLContext, returning a dataframe, if False,
            will use bare sparkContext, returning RDD (list-like)
        method: str
            Name of spark method to invoke
        context_kwargs: dict
            Used to create spark context and session *if* they do not already
            exist globally on this class.
        conf: dict
            Becomes SparkConf.
        """
        self.method = method
        self.sql = sql
        self.args = args
        self.kwargs = kwargs
        self.context_kwargs = context_kwargs
        self.setup()

    def __getstate__(self):
        fields = ['method', 'sql', 'args', 'kwargs', 'context_kwargs']
        return {m: getattr(self, m) for m in fields}

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.setup()

    def setup(self):
        """Call spark to instantiate resource"""
        if self.sc[0] is None:
            self.set_context()
        if self.sql:
            if self.sql[0] is None:
                self.set_sql()
            m = getattr(self.session[0], self.method)
        else:
            m = getattr(self.sc[0], self.method)
        return m(*self.args, **self.kwargs)
