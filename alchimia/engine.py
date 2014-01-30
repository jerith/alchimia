from sqlalchemy.engine.base import Engine

from twisted.internet.threads import deferToThreadPool

from alchimia.threadpool import ThreadPoolPool


class TwistedEngine(object):
    def __init__(self, pool, dialect, url, reactor=None, **kwargs):
        if reactor is None:
            raise TypeError("Must provide a reactor")

        super(TwistedEngine, self).__init__()
        self._engine = Engine(pool, dialect, url, **kwargs)
        self._reactor = reactor
        self._threadpoolpool = ThreadPoolPool()

    def _defer_to_thread(self, tpool, f, *args, **kwargs):
        print tpool
        return deferToThreadPool(self._reactor, tpool, f, *args, **kwargs)

    @property
    def dialect(self):
        return self._engine.dialect

    @property
    def _has_events(self):
        return self._engine._has_events

    @property
    def _execution_options(self):
        return self._engine._execution_options

    def _should_log_info(self):
        return self._engine._should_log_info()

    def _release_tpool_cb(self, result, tpool):
        self._threadpoolpool.release(tpool)
        return result

    def connect(self):
        tpool = self._threadpoolpool.acquire()
        d = self._defer_to_thread(tpool, self._engine.connect)
        d.addCallback(TwistedConnection, self, tpool)
        return d

    def execute(self, *args, **kwargs):
        tpool = self._threadpoolpool.acquire()
        d = self._defer_to_thread(tpool, self._engine.execute, *args, **kwargs)
        d.addCallback(TwistedResultProxy, self, tpool)
        d.addBoth(self._release_tpool_cb, tpool)
        return d

    def has_table(self, table_name, schema=None):
        tpool = self._threadpoolpool.acquire()
        d = self._defer_to_thread(
            tpool, self._engine.has_table, table_name, schema)
        d.addBoth(self._release_tpool_cb, tpool)
        return d

    def table_names(self, schema=None, connection=None):
        if connection is not None:
            tpool = connection._threadpool
            connection = connection._connection
        else:
            tpool = self._threadpoolpool.acquire()
        d = self._defer_to_thread(
            tpool, self._engine.table_names, schema, connection)
        if connection is None:
            d.addBoth(self._release_tpool_cb, tpool)
        return d


class TwistedConnection(object):
    def __init__(self, connection, engine, threadpool):
        super(TwistedConnection, self).__init__()
        self._connection = connection
        self._engine = engine
        self._threadpool = threadpool

    def _defer_to_thread(self, f, *args, **kwargs):
        return self._engine._defer_to_thread(
            self._threadpool, f, *args, **kwargs)

    def execute(self, *args, **kwargs):
        d = self._defer_to_thread(self._connection.execute, *args, **kwargs)
        d.addCallback(TwistedResultProxy, self._engine, self._threadpool)
        return d

    def close(self, *args, **kwargs):
        d = self._defer_to_thread(self._connection.close, *args, **kwargs)
        d.addCallback(self._engine._release_tpool_cb, self._threadpool)
        return d

    @property
    def closed(self):
        return self._connection.closed

    def begin(self, *args, **kwargs):
        d = self._defer_to_thread(
            self._connection.begin, *args, **kwargs)
        d.addCallback(TwistedTransaction, self)
        return d

    def in_transaction(self):
        return self._connection.in_transaction()


class TwistedTransaction(object):
    def __init__(self, transaction, connection):
        super(TwistedTransaction, self).__init__()
        self._transaction = transaction
        self._connection = connection

    def commit(self):
        return self._connection._defer_to_thread(self._transaction.commit)

    def rollback(self):
        return self._connection._defer_to_thread(self._transaction.rollback)

    def close(self):
        return self._connection._defer_to_thread(self._transaction.close)


class TwistedResultProxy(object):
    def __init__(self, result_proxy, engine, threadpool):
        super(TwistedResultProxy, self).__init__()
        self._result_proxy = result_proxy
        self._engine = engine
        self._threadpool = threadpool

    def _defer_to_thread(self, f, *args, **kwargs):
        return self._engine._defer_to_thread(
            self._threadpool, f, *args, **kwargs)

    def fetchone(self):
        return self._defer_to_thread(self._result_proxy.fetchone)

    def fetchall(self):
        return self._defer_to_thread(self._result_proxy.fetchall)

    def scalar(self):
        return self._defer_to_thread(self._result_proxy.scalar)

    def first(self):
        return self._defer_to_thread(self._result_proxy.first)

    def keys(self):
        return self._defer_to_thread(self._result_proxy.keys)

    @property
    def returns_rows(self):
        return self._result_proxy.returns_rows

    @property
    def rowcount(self):
        return self._result_proxy.rowcount
