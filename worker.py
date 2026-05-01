import os
from rq import Worker, Queue, Connection
from redis_conn import conn

listen = ['default']

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        worker.work()
