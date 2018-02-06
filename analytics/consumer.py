import logging, os
from threading import Thread

from analytics.version import VERSION
from analytics.request import post

try:
    from queue import Empty
except:
    from Queue import Empty

log = logging.getLogger('segment')


class AbstractConsumer(object):

    def __init__(self, queue, write_key, upload_size=100, host=None, on_error=None):
        """Create a consumer thread."""
        self.upload_size = upload_size
        self.write_key = write_key
        self.host = host
        self.on_error = on_error
        self.queue = queue
        self.retries = 3
        self.block_queue = False

    def upload(self):
        """Upload the next batch of items, return whether successful."""
        success = False
        batch = self.next()
        if len(batch) == 0:
            return False

        try:
            self.request(batch)
            success = True
        except Exception as e:
            log.error('error uploading: %s', e)
            success = False
            if self.on_error:
                self.on_error(e, batch)
        finally:
            # mark items as acknowledged from queue
            for item in batch:
                self.queue.task_done()
            return success

    def next(self):
        """Return the next batch of items to upload."""
        queue = self.queue
        items = []

        while len(items) < self.upload_size:
            try:
                item = queue.get(block=self.block_queue, timeout=self.timeout)
                if item:
                    items.append(item)
            except Empty:
                break

        return items

    def request(self, batch, attempt=0):
        """Attempt to upload the batch and retry before raising an error """
        try:
            post(self.write_key, self.host, batch=batch)
        except:
            if attempt > self.retries:
                raise
            self.request(batch, attempt + 1)


class AsyncConsumer(AbstractConsumer, Thread):
    """Consumes the messages from the client's queue."""

    def __init__(self, queue, write_key, upload_size=100, host=None, on_error=None):
        """Create a consumer thread."""
        Thread.__init__(self)
        AbstractConsumer.__init__(self, queue, write_key, upload_size, host, on_error)
        # Make consumer a daemon thread so that it doesn't block program exit
        # This does not work on AppEngine
        self.daemon = True
        # It's important to set running in the constructor: if we are asked to
        # pause immediately after construction, we might set running to True in
        # run() *after* we set it to False in pause... and keep running forever.
        self.retries = 3
        self.block_queue = True
        self.timeout = 0.5

    def run(self):
        """Runs the consumer."""
        log.debug('consumer is running...')
        while self.running:
            self.upload()
        log.debug('consumer exited.')

    def pause(self):
        """Pause (Exit) the consumer."""
        self.running = False

    def flush(self):
        # Empty message to wake up the waiting Thread
        self.queue.put(None, block=False)
        self.queue.join()


class SyncConsumer(AbstractConsumer):
    def on_error(self, e, batch):
        """On SyncConsumer bubble Exceptions by default."""
        raise

    def start(self):
        """On SyncConsumer start() is a NOOP."""
        pass

    def run(self):
        """Inform on how to use us."""
        log.info('Sync Mode, use `analytics.flush()` to send data to server.')

    def flush(self):
        """Flush the queue to the server."""
        log.info('Flushing to Server.')
        while not self.queue.empty():
            self.upload()

    def pause(self):
        """Flush the queue to the server."""
        self.running = False
        self.flush

    def join(self):
        self.pause()


def discover_default_consumer():
    # Google App Engine
    # https://cloud.google.com/appengine/docs/python/how-requests-are-handled#Python_The_environment
    if 'CURRENT_VERSION_ID' in os.environ and 'INSTANCE_ID' in os.environ:
        log.info('Detected environment to be Google App Engine. Using synchronous Mode.')
        return SyncConsumer

    # AWS Lambda
    # https://alestic.com/2014/11/aws-lambda-environment/
    if 'LAMBDA_TASK_ROOT' in os.environ:
        log.info('Detected environment to be AWS Lambda. Using synchronous HTTP transport.')
        return SyncConsumer

    return AsyncConsumer


Consumer = discover_default_consumer()
