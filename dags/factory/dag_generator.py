import logging
import os
import json

from dags.factory.create_dags_instances import Dag, Variable


class DagGenerator(Dag):
    """
    - This class creates Dag by passing a json config file's directory
    """

    def __init__(self, extractor_config, dag_config_extractor, task_builder, utility_builder):
        super().__init__(task=task_builder, utility=utility_builder)
        self.configs = []
        self.dag_ids = []
        self.duplicate_dag_ids = set()
        self.CONFIG_DIR = config_dir_path
        self.env = Variable.get("ENV")
        self.dag_extractor = dag_config_extractor
        self.dag_extractor_config = extractor_config

    def fetch_dag_configs(self):
        """
         -- fetches all json config file from given directory
         -- remove duplicate dag id's , report errors if any
        """
        self.configs = self.dag_extractor.extract(self.dag_extractor_config)
        pass

    def generate_dags(self, global_var):
        """
         - this function reads each config file and returns Dag object
        """
        self.fetch_dag_configs()
        for config in self.configs:
            try:
                dag_id = config['dag_id']
                if dag_id in self.duplicate_dag_ids:
                    raise RuntimeError('Multiple config files found with DAG ID "{}"'.format(dag_id))
                else:
                    dag = self.create_dag(config)
                    global_var[dag.dag_id] = dag
                    logging.info(f"DAG CREATION SUCCESS {dag.dag_id}")
            except Exception as e:
                logging.exception(f"Dag {config['dag_id']} raised exception at the time of parsing file")
                raise DynamicDagConfigurationException(config['dag_id']) from e
