from twisted.internet.interfaces import IReactorThreads
from twisted.python.failure import Failure

from zope.interface import implementer


@implementer(IReactorThreads)
class FakeThreadedReactor(object):
    def getThreadPool(self):
        return FakeThreadPool()

    def callFromThread(self, f, *args, **kwargs):
        return f(*args, **kwargs)

    def addSystemEventTrigger(self, phase, eventType, callable, *args, **kw):
        pass


class FakeThreadPool(object):
    joined = False

    def __init__(self, minthreads=5, maxthreads=20, name=None):
        self.name = name

    def start(self):
        self.joined = False

    def stop(self):
        self.joined = True

    def callInThreadWithCallback(self, cb, f, *args, **kwargs):
        try:
            result = f(*args, **kwargs)
        except Exception as e:
            cb(False, Failure(e))
        else:
            cb(True, result)
