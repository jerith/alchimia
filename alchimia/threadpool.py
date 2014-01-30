from twisted.python.threadpool import ThreadPool


class ThreadPoolPool(object):
    """
    This is a pool of threadpools.
    """

    maxpools = 20
    threadPoolFactory = ThreadPool

    def __init__(self, maxpools=20, minthreads=1, maxthreads=1):
        self.maxpools = maxpools
        self.minthreads = minthreads
        self.maxthreads = maxthreads
        self.pools = []

    def acquire(self):
        if len(self.pools) >= self.maxpools:
            raise Exception('There are already too many pools: %s of %s' % (
                len(self.pools), self.maxpools))
        newPool = self.threadPoolFactory(
            minthreads=self.minthreads, maxthreads=self.maxthreads)
        self.pools.append(newPool)
        newPool.start()
        return newPool

    def release(self, pool):
        self.pools.remove(pool)
        pool.stop()
