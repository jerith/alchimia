import weakref

from sqlalchemy.pool import SingletonThreadPool
from twisted.internet.defer import fail
from twisted.internet.threads import deferToThreadPool
from twisted.python import context
from twisted.python.failure import Failure
from twisted.python.threadpool import ThreadPool


class ConnectionThreadPoolContainer(object):
    """
    Container for a connection threadpool in Connection.info so we can keep a
    weakref to it and notice when the connection goes away.

    NOTE: Do not keep references to these outside of Connection.info or
          threadpools will not be properly cleaned up.
    """

    def __init__(self, threadpool):
        self.threadpool = threadpool


class ConnectionThreadPoolManagerBase(object):
    def __init__(self, reactor, threadpool_class=None):
        if threadpool_class is None:
            threadpool_class = ThreadPool

        self._reactor = reactor
        self._threadpool_class = threadpool_class
        self._threadpools = set()
        self._container_refs = {}
        self._connection_record_refs = {}
        reactor.addSystemEventTrigger(
            'before', 'shutdown', self.kill_all_threadpools)

    def new_threadpool(self):
        raise NotImplementedError()

    def kill_threadpool(self, threadpool, new_connection=False):
        raise NotImplementedError()

    def kill_all_threadpools(self):
        raise NotImplementedError()

    def _new_threadpool(self):
        tpool = self._threadpool_class(minthreads=1, maxthreads=1)
        self._threadpools.add(tpool)
        tpool.start()
        return tpool

    def add_to_connection(self, threadpool, connection):
        if '_alchimia_threadpool' in connection.info:
            # Nothing to do here.
            return
        container = ConnectionThreadPoolContainer(threadpool)
        connection.info['_alchimia_threadpool'] = container
        container_ref = weakref.ref(container, self._container_ref_callback)
        self._container_refs[container_ref] = threadpool
        connection_record_ref = weakref.ref(
            connection._connection_record,
            self._connection_record_ref_callback)
        self._connection_record_refs[connection_record_ref] = threadpool
        threadpool._alchimia_should_stop = False

    def _container_ref_callback(self, container_ref):
        tpool = self._container_refs.pop(container_ref, None)
        if tpool is not None:
            self._reactor.callFromThread(self.kill_threadpool, tpool)

    def _connection_record_ref_callback(self, connection_record_ref):
        tpool = self._connection_record_refs.pop(connection_record_ref, None)
        if tpool is not None:
            self._reactor.callFromThread(self.kill_threadpool, tpool)

    def defer_to_connection_thread(self, conn, f, *args, **kwargs):
        try:
            tpool = conn.info['_alchimia_threadpool'].threadpool
        except Exception as e:
            return fail(Failure(e))
        return deferToThreadPool(self._reactor, tpool, f, *args, **kwargs)

    def _stop_tpool_if_necessary_cb(self, result, threadpool):
        if threadpool._alchimia_should_stop:
            self.kill_threadpool(threadpool, new_connection=True)
        return result

    def defer_to_new_connection_thread(self, f, *args, **kwargs):
        tpool = self.new_threadpool()
        tpool._alchimia_should_stop = True
        add_to_connection = lambda conn: self.add_to_connection(tpool, conn)
        ctx = {'_alchimia_connthread_func': add_to_connection}

        d = context.call(
            ctx, deferToThreadPool, self._reactor, tpool, f, *args, **kwargs)
        d.addBoth(self._stop_tpool_if_necessary_cb, tpool)
        return d


class ConnectionThreadPoolManager(ConnectionThreadPoolManagerBase):
    def __init__(self, reactor, threadpool_class=None):
        super(ConnectionThreadPoolManager, self).__init__(
            reactor, threadpool_class=threadpool_class)
        self._threadpools = set()

    def new_threadpool(self):
        tpool = self._new_threadpool()
        self._threadpools.add(tpool)
        return tpool

    def kill_threadpool(self, threadpool, new_connection=False):
        self._threadpools.discard(threadpool)
        threadpool.stop()

    def kill_all_threadpools(self):
        while self._threadpools:
            self.kill_threadpool(self._threadpools.pop())


class SingletonConnectionThreadPoolManager(ConnectionThreadPoolManagerBase):
    def __init__(self, reactor, threadpool_class=None):
        super(SingletonConnectionThreadPoolManager, self).__init__(
            reactor, threadpool_class=threadpool_class)
        self._threadpool = None

    def new_threadpool(self):
        if self._threadpool is None:
            self._threadpool = self._threadpool_class(
                minthreads=1, maxthreads=1)
            self._threadpool.start()
        return self._threadpool

    def kill_threadpool(self, threadpool, new_connection=False):
        if new_connection:
            return
        if threadpool is self._threadpool:
            self._threadpool = None
        threadpool.stop()

    def kill_all_threadpools(self):
        if self._threadpool is not None:
            self.kill_threadpool(self._threadpool)


class ManagedThreadConnectionPoolWrapper(object):
    def __init__(self, pool):
        self._pool = pool

    def _new_connection(self, func):
        conn = func()
        connthread_func = context.get('_alchimia_connthread_func')
        connthread_func(conn)
        return conn

    def connect(self):
        return self._new_connection(self._pool.connect)

    def unique_connection(self):
        return self._new_connection(self._pool.unique_connection)

    def dispose(self):
        return self._pool.dispose()

    def recreate(self):
        return type(self)(self._pool.recreate())

    def _replace(self):
        return type(self)(self._pool._replace())


def threadpool_manager_for_pool(pool, reactor, threadpool_class):
    if isinstance(pool, SingletonThreadPool):
        return SingletonConnectionThreadPoolManager(reactor, threadpool_class)
    else:
        return ConnectionThreadPoolManager(reactor, threadpool_class)
