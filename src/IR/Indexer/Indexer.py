from elasticsearch import Elasticsearch, helpers
import subprocess
import os

from src.IR.config.Elasic_Config_Loader import Elasic_Config_Loader


class Indexer:
    def __init__(self, index_name=None):
        """
        Initialize the indexer. Make sure the elastic search is running.
        :param index_name: No need to pass this parameter unless some particular reason. It will be loaded from config file.
        """

        # Create an instance of ConfigLoader (config file will be loaded automatically)
        self.bulk_index_array = []
        config_loader = Elasic_Config_Loader()

        # Accessing configuration parameters using class methods
        elastic_search_host = config_loader.get_elastic_search_host()
        elastic_search_port = config_loader.get_elastic_search_port()
        self.elastic_search_index = config_loader.get_index_name()

        if index_name is None:
            self.index_name = self.elastic_search_index
        else:
            self.index_name = index_name

        # Create an instance of Elasticsearch client
        self.es_client = Elasticsearch('http://' + elastic_search_host + ':' + str(elastic_search_port),
                                  # http_auth=("username", "password"),
                                  verify_certs=False)

    def checkout_commit_before_fix(self, repo_path, fix_commit):
        """
        Checkout the commit before the fix commit
        :param repo_path: Path to the git repository
        :param fix_commit: The commit hash where the bug was fixed
        :return: True if successful, False otherwise
        """
        try:
            # Change to the repository directory
            original_dir = os.getcwd()
            os.chdir(repo_path)
            
            # Get the commit before the fix commit
            result = subprocess.run(['git', 'rev-parse', f'{fix_commit}^'], 
                                  capture_output=True, text=True, check=True)
            commit_before_fix = result.stdout.strip()
            
            # Checkout the commit before the fix
            subprocess.run(['git', 'checkout', commit_before_fix], check=True)
            
            print(f"Successfully checked out commit {commit_before_fix} (before fix commit {fix_commit})")
            
            # Return to original directory
            os.chdir(original_dir)
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"Error checking out commit: {e}")
            os.chdir(original_dir)
            return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            os.chdir(original_dir)
            return False

    def index(self, project, source_code, file_url, fixed_commit):
        document = {
            "project": project,
            "source_code": source_code,
            "file_url": file_url,
            "fixed_commit": fixed_commit
        }
        result = self.es_client.index(index=self.index_name, body=document, refresh=False)
        print(f"Indexed document with ID: {result['_id']}")

        return result

    def bulk_action(self):
        for document in self.bulk_index_array:
            yield document

    # function for bulk indexing. it is same as before. saves the doc in array and when it reaches the limit, it indexes them in bulk
    def bulk_index(self, project, source_code, file_url, fixed_commit, bulk_size=1024):
        document = {
            "project": project,
            "source_code": source_code,
            "file_url": file_url,
            "fixed_commit": fixed_commit
        }

        indexable_document = {
            "_index": self.index_name,
            "_source": document
        }
        self.bulk_index_array.append(indexable_document)

        if len(self.bulk_index_array) >= bulk_size:
            helpers.bulk(self.es_client, actions=self.bulk_action())
            # print(f"Indexed {bulk_size} documents in bulk.")
            self.bulk_index_array = []

    def refresh(self):
        if len(self.bulk_index_array) > 0:
            helpers.bulk(self.es_client, actions=self.bulk_action())
            # print(f"Indexed {len(self.bulk_index_array)} documents in bulk.")
            self.bulk_index_array = []

        self.es_client.indices.refresh(index=self.index_name)

    def __del__(self):
        self.refresh()
