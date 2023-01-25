import importlib
import logging
from datetime import datetime
import pendulum
import copy
import os
from airflow.models import DAG, Variable


DEFAULT_ARGS_TEMPLATE = {
    "owner": "airflow",
    "email": [],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0
}


class Dag():
    """
     -- This class generate Dags from  given json config
    """
    config = None
    dag = None
    task_names = None
    default_args = None
    custom_dag_default_args = None

    def __init__(self, utility, task):
        self.utility_builder = utility
        self.task_builder = task

    def process_config(self):
        """
        process_config parses the given json config
          - creates airflow task's dependencies(*args,**kwargs)
          - creates different Utilities dependencies(*args,**kwargs)
          - And appends these dependencies to its config , to them while buiding Dag's task
        """
        pass

    def append_task(self):

        """
         - Creates new Dag definition
         - creates new Airflow Task definition and, appends it to Dag object
        """
        with DAG(
            dag_id=dag_id,
            description=self.config.get("description") or dag_id,
            default_args=self.default_args,
            schedule_interval=self.config.get("cron_tab"),
            start_date=datetime(2021, 1, 1, tzinfo=pendulum.timezone(local_tz)),
            catchup=False,
            max_active_runs=1,
            tags=tags
        ) as dag:
            tasks = []
            for task_name in self.task_names:
                task = self.task_builder.create_task(task_name, self.config)
                task.dag = dag
                tasks.append(task)

            # NOTE: this does not support branched workflows
            for i in range(len(tasks) - 1):
                tasks[i].set_downstream(tasks[i + 1])

        return dag

    def create_dag(self, config):
        self.config = copy.deepcopy(config)
        self.process_config()
        dag = self.append_task()
        return dag
