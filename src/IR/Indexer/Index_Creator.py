from elasticsearch import Elasticsearch
import os
import xml.etree.ElementTree as ET
from tqdm import tqdm

from src.IR.config.Elasic_Config_Loader import Elasic_Config_Loader
from src.IR.Indexer.Indexer import Indexer

'''
    CAUTION:
    This is used to create an index in elastic search. This needs to be run only once. running it again will delete the index and create a new one.
    Before running this, make sure to update the config file and fields. 
    
    Before running this, make sure elastic search is running. See readme for more details.
'''


class Index_Creator:
    def __init__(self):
        # Create an instance of ConfigLoader (config file will be loaded automatically)
        self.config_loader = Elasic_Config_Loader("../config/IR_config.yaml")
        # self.general_config_loader = ConfigLoader("../config/IR_config.yaml")

        # Accessing configuration parameters using class methods
        self.elastic_search_host = self.config_loader.get_elastic_search_host()
        self.elastic_search_port = self.config_loader.get_elastic_search_port()
        elastic_search_index = self.config_loader.get_index_name()
        # self.embedding_dimension = self.general_config_loader.get_value("Embedding", "dimension")

        self.index_name = elastic_search_index

        self.fields = self.config_loader.get_index_fields()

        # get the name of the fields as a list
        self.fields_names = list(self.fields.keys())

        # Create an instance of Elasticsearch client
        self.es_client = Elasticsearch(
            'http://' + self.elastic_search_host + ':' + str(self.elastic_search_port),
            verify_certs=False
        )

    def create_index(self, delete_if_exists=False):
        config = {
            "mappings": {
                "properties": {
                    field_name: {"type": field_type}
                    for field_name, field_type in self.fields.items()
                }
            },
            "settings": {
                "index": {
                    "number_of_shards": 3,
                    "number_of_replicas": 0,
                    "refresh_interval": "30s",
                    # 'index_buffer_size': '512mb',
                }
            }
        }

        index_exists = self.es_client.indices.exists(index=self.index_name)

        # Print the result
        if index_exists:
            print(f"Index '{self.index_name}' already exists.")

            if delete_if_exists:
                print(f"Deleting index '{self.index_name}'.")
                response = self.es_client.indices.delete(index=self.index_name)
                # Check if the deletion was successful
                if response['acknowledged']:
                    print(f"The index '{self.index_name}' was successfully deleted.")
                else:
                    print(f"Failed to delete the index '{self.index_name}'.")
            else:
                print(f"Index '{self.index_name}' will not be deleted.")
                return
        else:
            print(f"Index '{self.index_name}' does not exist.")

        self.es_client.indices.create(
            index=self.index_name,
            mappings=config["mappings"],
            settings=config["settings"]
        )

        # Check if the index has been created successfully
        if self.es_client.indices.exists(index=self.index_name):
            print(f"The index '{self.index_name}' was created successfully.")
        else:
            print(f"Failed to create the index '{self.index_name}'.")

    def parse_xml_dataset_for_commits(self, xml_file_path):
        """
        Parse the XML dataset to extract unique commit hashes
        """
        print(f"Parsing XML dataset to extract commits: {xml_file_path}")
        
        # Read the XML file
        with open(xml_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Parse the XML content
        root = ET.fromstring(content)
        
        commits = set()
        
        # Find all table elements (each represents a bug)
        for table in root.findall('.//table'):
            # Extract commit from column elements
            for column in table.findall('column'):
                name = column.get('name')
                value = column.text.strip() if column.text else ""
                
                if name == 'commit':
                    commits.add(value)
                    break
        
        commits_list = list(commits)
        print(f"Found {len(commits_list)} unique commits in dataset")
        return commits_list

    def index_all_commits_from_dataset(self, source_code_path, project_name, xml_dataset_path):
        """
        Index source code for all commits found in the dataset
        :param source_code_path: Path to the git repository
        :param project_name: Name of the project
        :param xml_dataset_path: Path to the XML dataset file
        """
        print(f"Starting to index all commits from dataset")
        print(f"Source code path: {source_code_path}")
        print(f"Project name: {project_name}")
        print(f"Dataset path: {xml_dataset_path}")
        
        # Parse the dataset to get all unique commits
        commits = self.parse_xml_dataset_for_commits(xml_dataset_path)
        
        if not commits:
            print("No commits found in dataset, exiting...")
            return False
        
        # Index each commit
        successful_indexes = 0
        failed_indexes = 0
        
        for commit in tqdm(commits, desc="Indexing commits"):
            try:
                success = self.index_source_code_for_commit(source_code_path, project_name, commit)
                if success:
                    successful_indexes += 1
                else:
                    failed_indexes += 1
            except Exception as e:
                print(f"Error indexing commit {commit}: {e}")
                failed_indexes += 1
                continue
        
        print(f"Indexing completed!")
        print(f"Successfully indexed: {successful_indexes} commits")
        print(f"Failed to index: {failed_indexes} commits")
        
        return successful_indexes > 0

    def index_source_code_for_commit(self, source_code_path, project_name, fixed_commit):
        """
        Index source code for a specific commit (the commit before the fix)
        :param source_code_path: Path to the directory containing Java source files
        :param project_name: Name of the project to use in the index
        :param fixed_commit: The commit hash where the bug was fixed
        """
        print(f"Starting to index source code from: {source_code_path}")
        print(f"Project name: {project_name}")
        print(f"Fixed commit: {fixed_commit}")
        
        # Create an instance of the Indexer
        indexer = Indexer()
        
        # First, checkout the commit before the fix
        if not indexer.checkout_commit_before_fix(source_code_path, fixed_commit):
            print(f"Failed to checkout commit before {fixed_commit}, skipping indexing...")
            return False
        
        # Counter for indexed files
        indexed_count = 0
        
        # Walk through the source code directory
        for root, dirs, files in tqdm(os.walk(source_code_path), desc="Scanning directories"):
            for file in files:
                if file.endswith('.java'):
                    file_path = os.path.join(root, file)
                    
                    try:
                        # Read the source code file
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            source_code = f.read()
                        
                        # Get the relative file URL (path from source_code_path)
                        file_url = os.path.relpath(file_path, source_code_path)
                        
                        # Index the file using bulk indexing for better performance
                        indexer.bulk_index(
                            project=project_name,
                            source_code=source_code,
                            file_url=file_url,
                            fixed_commit=fixed_commit
                        )
                        
                        indexed_count += 1
                        
                    except Exception as e:
                        print(f"Error reading file {file_path}: {e}")
                        continue
        
        # Refresh the indexer to flush any remaining documents
        indexer.refresh()
        
        print(f"Successfully indexed {indexed_count} Java files for project: {project_name}, fixed_commit: {fixed_commit}")
        return True

    def index_source_code(self, source_code_path, project_name):
        """
        Legacy method for backward compatibility - indexes current state of source code
        :param source_code_path: Path to the directory containing Java source files
        :param project_name: Name of the project to use in the index
        """
        print(f"Starting to index source code from: {source_code_path}")
        print(f"Project name: {project_name}")
        
        # Create an instance of the Indexer
        indexer = Indexer()
        
        # Counter for indexed files
        indexed_count = 0
        
        # Walk through the source code directory
        for root, dirs, files in tqdm(os.walk(source_code_path), desc="Scanning directories"):
            for file in files:
                if file.endswith('.java'):
                    file_path = os.path.join(root, file)
                    
                    try:
                        # Read the source code file
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            source_code = f.read()
                        
                        # Get the relative file URL (path from source_code_path)
                        file_url = os.path.relpath(file_path, source_code_path)
                        
                        # Index the file using bulk indexing for better performance
                        indexer.bulk_index(
                            project=project_name,
                            source_code=source_code,
                            file_url=file_url,
                            fixed_commit="current"  # Use "current" for legacy indexing
                        )
                        
                        indexed_count += 1
                        
                    except Exception as e:
                        print(f"Error reading file {file_path}: {e}")
                        continue
        
        # Refresh the indexer to flush any remaining documents
        indexer.refresh()
        
        print(f"Successfully indexed {indexed_count} Java files for project: {project_name}")
        return True


if __name__ == '__main__':
    index_creator = Index_Creator()
    index_creator.create_index(delete_if_exists=True)
    
    # Index all commits from the dataset
    source_code_path = "/Users/armin/Desktop/test-aspect/aspectj"
    project_name = "aspectj"
    xml_dataset_path = "/Users/armin/Desktop/UCI/bug-localization-project/Codes/Adjusted-BRaIn/Adjusted-BraIn/Data/ye et al/aspectj.xml"
    index_creator.index_all_commits_from_dataset(source_code_path, project_name, xml_dataset_path)
