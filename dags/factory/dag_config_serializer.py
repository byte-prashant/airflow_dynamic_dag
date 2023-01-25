from abc import ABC, abstractmethod
import json
import logging


class ConfigExtractorInterface(ABC):

    @abstractmethod
    def extract(self, config):
        pass


class JSONExtractor(ConfigextractorInterface):

    def extract(self, config):
        try:
            return True, [json.loads(config)]
        except Exception :
            logging.exception("This extractor can not be used , as config is not json string")
            return False, "This extractor can not be used , as  config is not json string"


class FileExtractor(ConfigextractorInterface):

    def extract(self, config):
        try:
            with open(config) as f:
                json_settings = json.load(f)
                logging.info("JSON SETTING FILE IMPORTED!!")
                return True, [json_settings]
        except Exception as e :
            logging.exception("This extractor can not be used , unable to read file path")
            return False, f"This extractor can not be used , as  unable to read file path {str(e)}"


class FilePathExtractor(ConfigextractorInterface):

    def extract(self, path):
        """
            -- fetches all json config file from given directory
        """
        configs = []
        for filename in os.listdir(self.path):
            file_path = os.path.join(self.path, filename)
            with open(file_path) as config_file:
                try:
                    config = json.load(config_file)
                    configs.append(config)
                except Exception as e:
                    logging.warning(
                        f"DAG CREATION ERROR - Config: {config['config_file_path']}, Error: {e}")

                    return False, f"This extractor can not be used , as  unable to read file path {str(e)}"

        return True, configs


class MangoDBExtractor(ConfigextractorInterface):

    def get_db_connection(self, config):
        """
            - return db connection

        """
        return "db_connection"

    def extract(self, config):
        """

            -- fetches all json config file from given directory
        """
        configs = []
        db = self.get_db_connection(config)
        ## config = db.get_configs()
        return True, configs