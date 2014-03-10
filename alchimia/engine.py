from sqlalchemy.engine.base import Engine

from alchimia.threads import (
    ConnectionThreadPoolManager, ManagedThreadConnectionPoolWrapper
)


def _conn_execute_cb(connection, *args, **kwargs):
    return connection.execute(*args, **kwargs)


def _run_and_close_cb(_connection, callable_, *args, **kwargs):
    return _connection._run_callable_and_close(callable_, *args, **kwargs)


def _close_connection_cb(result, connection):
    d = connection.close()
    d.addBoth(lambda _: result)
    return d


class TwistedEngine(object):
    def __init__(self, pool, dialect, url, reactor=None, threadpool_class=None,
                 **kwargs):
        if reactor is None:
            raise TypeError("Must provide a reactor")

        pool = ManagedThreadConnectionPoolWrapper(pool)
        self._engine = Engine(pool, dialect, url, **kwargs)
        self._reactor = reactor
        self._threadpool_manager = ConnectionThreadPoolManager(
            reactor, threadpool_class)

    def _defer_to_connection_thread(self, conn, f, *args, **kwargs):
        return self._threadpool_manager.defer_to_connection_thread(
            conn, f, *args, **kwargs)

    def _defer_to_new_connection_thread(self, f, *args, **kwargs):
        return self._threadpool_manager.defer_to_new_connection_thread(
            f, *args, **kwargs)

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

    def dispose(self):
        self._engine.dispose()

    def connect(self, **kwargs):
        d = self._defer_to_new_connection_thread(
            self._engine.connect, **kwargs)
        d.addCallback(TwistedConnection, self)
        return d

    def contextual_connect(self, *args, **kwargs):
        d = self._defer_to_new_connection_thread(
            self._engine.contextual_connect, *args, **kwargs)
        d.addCallback(TwistedConnection, self)
        return d

    def execute(self, *args, **kwargs):
        d = self.contextual_connect()
        d.addCallback(_conn_execute_cb, *args, **kwargs)
        return d

    def run_callable(self, callable_, *args, **kwargs):
        d = self.contextual_connect()
        d.addCallback(_run_and_close_cb, callable_, *args, **kwargs)
        return d

    def has_table(self, table_name, schema=None):
        return self.run_callable(
            self._engine.dialect.has_table, table_name, schema)

    def table_names(self, schema=None, connection=None):
        if connection is not None:
            return connection._defer_to_thread(
                self._engine.table_names, schema=schema,
                connection=connection._connection)

        d = self.contextual_connect()
        d.addCallback(self._table_names_cb, schema)
        return d

    def _table_names_cb(self, connection, schema):
        d = connection._defer_to_thread(
            self._engine.table_names, schema=schema,
            connection=connection._connection)
        d.addBoth(_close_connection_cb, connection)
        return d


class TwistedConnection(object):
    def __init__(self, connection, engine):
        self._connection = connection
        self._engine = engine

    def _defer_to_thread(self, callable_, *args, **kwargs):
        return self._engine._defer_to_connection_thread(
            self._connection, callable_, *args, **kwargs)

    def run_callable(self, callable_, *args, **kwargs):
        return self._defer_to_thread(
            callable_, self._connection, *args, **kwargs)

    def _run_callable_and_close(self, callable_, *args, **kwargs):
        d = self.run_callable(callable_, *args, **kwargs)
        d.addBoth(_close_connection_cb, self)
        return d

    def execute(self, *args, **kwargs):
        d = self._defer_to_thread(self._connection.execute, *args, **kwargs)
        d.addCallback(TwistedResultProxy, self._engine)
        return d

    def close(self, *args, **kwargs):
        return self._defer_to_thread(self._connection.close, *args, **kwargs)

    @property
    def closed(self):
        return self._connection.closed

    def begin(self, *args, **kwargs):
        d = self._defer_to_thread(self._connection.begin, *args, **kwargs)
        d.addCallback(TwistedTransaction, self._engine)
        return d

    def in_transaction(self):
        return self._connection.in_transaction()


class TwistedTransaction(object):
    def __init__(self, transaction, engine):
        self._transaction = transaction
        self._engine = engine

    def _defer_to_thread(self, f, *args, **kwargs):
        return self._engine._defer_to_connection_thread(
            self._transaction.connection, f, *args, **kwargs)

    def commit(self):
        return self._defer_to_thread(self._transaction.commit)

    def rollback(self):
        return self._defer_to_thread(self._transaction.rollback)

    def close(self):
        return self._defer_to_thread(self._transaction.close)


class TwistedResultProxy(object):
    def __init__(self, result_proxy, engine):
        self._result_proxy = result_proxy
        self._engine = engine

    def _defer_to_thread(self, f, *args, **kwargs):
        return self._engine._defer_to_connection_thread(
            self._result_proxy.connection, f, *args, **kwargs)

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
