from rq import Worker, Queue
from redis_conn import conn

listen = ['default']

if __name__ == '__main__':
    worker = Worker([Queue(name, connection=conn) for name in listen])
    worker.work()
