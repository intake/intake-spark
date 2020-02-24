import threading


class SparkHolder(object):
    sc = [None]
    session = [None]
    lock = threading.Lock()

    def set_context(self, sc=None, conf=None, master=None, app_name=None,
                    executor_env=None, spark_home=None, **kw):
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
        with self.lock:
            if sc is None:
                if self.sc[0] is not None:
                    return
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

    def set_session(self, session=None, hive=None, **kw):
        """Set global SQL SparkSession

        Parameters
        ----------
        session: SparkSession or None
            Explicitly provide a session object, if you have one
        hive: bool
            Whether to enable Hive support when creating session
        """
        with self.lock:
            if session is None:
                if self.session[0] is not None:
                    return
                from pyspark import sql
                if hive or self.hive:
                    session = sql.SparkSession.builder.enableHiveSupport(
                        ).getOrCreate()
                else:
                    session = sql.SparkSession.getOrCreate()
            self.session[0] = session

    @classmethod
    def set_class_session(cls, **context_kwargs):
        """Create or use spark session/context for instances of this class

        See the input parameters of ``set_context`` and ``set_session``.
        """
        inst = cls(True, (), context_kwargs)
        inst.set_context(**context_kwargs)
        inst.set_session(**context_kwargs)

    def __init__(self, sql, args, context_kwargs):
        """Create reference to spark resource

        Parameters
        ----------
        sql: bool
            If True, will use SQLContext (i.e., Session), returning a dataframe,
            if False, will use bare SparkContext, returning RDD (list-like)
        args: list or tuples
            Details of a sequence of spark methods to invoke.
            Each element is must be a tuple tuple, (method_name, args, kwargs),
            where method_name is a string corresponding to the attribute lookup
            to perform on the result of the previous stage. args and kwargs,
            if given, will be passed when calling the method; if not given,
            it is assumes to be a simple attribute.
            The starting object for the lookups is a SparkContext is sql=False,
            or a Spark Session if True.
        context_kwargs: dict
            Used to create spark context and session *if* they do not already
            exist globally on this class.
        """
        self.sql = sql
        self.args = args
        self.context_kwargs = context_kwargs or {}
        self.hive = self.context_kwargs.pop('hive', True)

    def __getstate__(self):
        fields = ['method', 'sql', 'args', 'kwargs', 'context_kwargs']
        return {m: getattr(self, m) for m in fields}

    def __setstate__(self, state):
        self.__dict__.update(state)

    def setup(self):
        """Call spark to instantiate resource"""
        if self.sc[0] is None:
            self.set_context(**self.context_kwargs)
        if self.sql:
            if self.session[0] is None:
                self.set_session(**self.context_kwargs)
            m = self.session[0]
        else:
            m = self.sc[0]
        for state in self.args:
            method = state[0]
            if len(state) == 1:
                m = getattr(m, method)
            else:
                args = state[1]
                if len(state) > 2:
                    kwargs = state[2]
                else:
                    kwargs = {}
                m = getattr(m, method)(*args, **kwargs)
        return m
