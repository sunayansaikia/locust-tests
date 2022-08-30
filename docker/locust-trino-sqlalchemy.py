from __future__ import absolute_import
from __future__ import print_function
from locust import User, between, TaskSet, task, events
from pyhive import presto
from sqlalchemy import create_engine
from sqlalchemy.schema import Table, MetaData
from sqlalchemy.sql.expression import select, text


import time

def create_conn():
    engine = create_engine(
        'trino://admin@trino-coordinator:8080/tpch'
    )
    
    presto_connection = engine.connect()

    if None != presto_connection:
        print("Connection successful")

    return presto_connection

def execute_presto_query(query):
    try:
        presto_connection = create_conn()
        rows = presto_connection.execute(text(query)).fetchall()
        return rows
    except Exception as e:
        print(format(e))


'''
  The Presto client that wraps the actual query
'''
class PrestoClient:

    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                res = execute_presto_query(*args, **kwargs)
                events.request_success.fire(request_type="presto",
                                            name=name,
                                            response_time=int((time.time() - start_time) * 1000),
                                            response_length=len(res))
            except Exception as e:
                events.request_failure.fire(request_type="presto",
                                            name=name,
                                            response_time=int((time.time() - start_time) * 1000),
                                            exception=e)

                print('error {}'.format(e))

        return wrapper
'''
  You can move this class to a different file and import it here
'''
class PrestoTaskSet(TaskSet):

    @task(1)
    def execute_presto_query(self):
        self.client.execute_presto_query("select count(1) from tiny.customer")

class PrestoLocust(User):
    min_wait = 0
    max_wait = 0
    task_set = PrestoTaskSet
    wait_time = between(min_wait, max_wait)
    tasks = [PrestoTaskSet]
    def __init__(self, environment):
        super().__init__(environment)
        self.client = PrestoClient()
