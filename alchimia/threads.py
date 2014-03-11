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


class ConnectionThreadPoolManager(object):
    def __init__(self, reactor, threadpool_class=None):
        if threadpool_class is None:
            threadpool_class = ThreadPool

        self._reactor = reactor
        self._threadpool_class = threadpool_class
        self._threadpools = set()
        self._container_refs = {}
        reactor.addSystemEventTrigger(
            'before', 'shutdown', self.kill_all_threadpools)

    def kill_threadpool(self, threadpool):
        self._threadpools.discard(threadpool)
        threadpool.stop()

    def kill_all_threadpools(self):
        while self._threadpools:
            self.kill_threadpool(self._threadpools.pop())

    def new_threadpool(self):
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
        threadpool._alchimia_should_stop = False

    def _container_ref_callback(self, container_ref):
        tpool = self._container_refs.pop(container_ref, None)
        if tpool is not None:
            self._reactor.callFromThread(self.kill_threadpool, tpool)

    def defer_to_connection_thread(self, conn, f, *args, **kwargs):
        try:
            tpool = conn.info['_alchimia_threadpool'].threadpool
        except Exception as e:
            return fail(Failure(e))
        return deferToThreadPool(self._reactor, tpool, f, *args, **kwargs)

    def defer_to_new_connection_thread(self, f, *args, **kwargs):
        tpool = self.new_threadpool()
        tpool._alchimia_should_stop = True
        add_to_connection = lambda conn: self.add_to_connection(tpool, conn)
        ctx = {'_alchimia_connthread_func': add_to_connection}

        def _stop_tpool_if_necessary(result):
            if tpool._alchimia_should_stop:
                tpool.stop()
            del tpool._alchimia_should_stop
            return result

        d = context.call(
            ctx, deferToThreadPool, self._reactor, tpool, f, *args, **kwargs)
        d.addBoth(_stop_tpool_if_necessary)
        return d


class SingletonConnectionThreadPoolManager(object):
    def __init__(self, reactor, threadpool_class=None):
        if threadpool_class is None:
            threadpool_class = ThreadPool

        self._reactor = reactor
        self._threadpool_class = threadpool_class
        self._threadpool = None
        self._container_refs = {}
        reactor.addSystemEventTrigger(
            'before', 'shutdown', self.kill_all_threadpools)

    def kill_threadpool(self, threadpool):
        if threadpool is self._threadpool:
            self._threadpool = None
        threadpool.stop()

    def kill_all_threadpools(self):
        if self._threadpool is not None:
            self.kill_threadpool(self._threadpool)

    def new_threadpool(self):
        if self._threadpool is None:
            self._threadpool = self._threadpool_class(
                minthreads=1, maxthreads=1)
            self._threadpool.start()
        return self._threadpool

    def add_to_connection(self, threadpool, connection):
        if '_alchimia_threadpool' in connection.info:
            # Nothing to do here.
            return
        container = ConnectionThreadPoolContainer(threadpool)
        connection.info['_alchimia_threadpool'] = container
        container_ref = weakref.ref(container, self._container_ref_callback)
        self._container_refs[container_ref] = threadpool

    def _container_ref_callback(self, container_ref):
        tpool = self._container_refs.pop(container_ref, None)
        if tpool is not None:
            self._reactor.callFromThread(self.kill_threadpool, tpool)

    def defer_to_connection_thread(self, conn, f, *args, **kwargs):
        try:
            tpool = conn.info['_alchimia_threadpool'].threadpool
        except Exception as e:
            return fail(Failure(e))
        return deferToThreadPool(self._reactor, tpool, f, *args, **kwargs)

    def defer_to_new_connection_thread(self, f, *args, **kwargs):
        tpool = self.new_threadpool()
        add_to_connection = lambda conn: self.add_to_connection(tpool, conn)
        ctx = {'_alchimia_connthread_func': add_to_connection}

        return context.call(
            ctx, deferToThreadPool, self._reactor, tpool, f, *args, **kwargs)


class ManagedThreadConnectionPoolWrapper(object):
    def __init__(self, pool):
        self._pool = pool

    def connect(self):
        conn = self._pool.connect()
        connthread_func = context.get('_alchimia_connthread_func')
        connthread_func(conn)
        return conn

    def dispose(self):
        return self._pool.dispose()

    def recreate(self):
        return type(self)(self._pool.recreate())

    def _replace(self):
        return type(self)(self._pool._replace())

    def unique_connection(self):
        conn = self._pool.unique_connection()
        connthread_func = context.get('_alchimia_connthread_func')
        connthread_func(conn)
        return conn


def threadpool_manager_for_pool(pool, reactor, threadpool_class):
    if isinstance(pool, SingletonThreadPool):
        return SingletonConnectionThreadPoolManager(reactor, threadpool_class)
    else:
        return ConnectionThreadPoolManager(reactor, threadpool_class)
