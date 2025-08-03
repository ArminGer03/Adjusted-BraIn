import json
import xml.etree.ElementTree as ET
import os

from tqdm import tqdm

from src.IR import Searcher
from src.Utils import JavaSourceParser
from src.Utils.IO import JSON_File_IO
from src.Utils.Parser.JavaSourceParser import clear_formatting

from py4j.java_gateway import JavaGateway


def parse_xml_dataset(file_path):
    """
    Parse the ye_et_al XML dataset format
    """
    print(f"Parsing XML dataset from: {file_path}")

    # Read the XML file
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Parse the XML content
    root = ET.fromstring(content)

    bugs = []

    # Find all table elements (each represents a bug)
    for table in root.findall('.//table'):
        bug = {}

        # Extract data from column elements
        for column in table.findall('column'):
            name = column.get('name')
            value = column.text.strip() if column.text else ""

            if name == 'bug_id':
                bug['bug_id'] = value
            elif name == 'summary':
                bug['bug_title'] = value
            elif name == 'description':
                bug['bug_description'] = value
            elif name == 'commit':
                bug['fixed_commit'] = value
            elif name == 'files':
                # Parse files field - it contains file paths separated by whitespace
                files = [f.strip() for f in value.split() if f.strip()]
                bug['fixed_files'] = files
            elif name == 'result':
                # Parse result field - contains file:line_number format
                results = []
                for line in value.split('\n'):
                    line = line.strip()
                    if ':' in line:
                        parts = line.split(':', 1)
                        if len(parts) == 2:
                            file_path = parts[0].strip()
                            line_number = parts[1].strip()
                            results.append({
                                'file': file_path,
                                'line': line_number
                            })
                bug['result'] = results

        # Only add bugs that have the required fields
        if 'bug_id' in bug and 'bug_title' in bug and 'bug_description' in bug and 'fixed_commit' in bug:
            bugs.append(bug)

    print(f"Parsed {len(bugs)} bugs from XML dataset")
    return bugs


def perform_search(project, fixed_commit, bug_title, bug_description, top_K_results=10):
    searcher = Searcher('ye_et_al')  # Use the ye_et_al index
    search_results = searcher.search_Extended(
        project=project,
        fixed_commit=fixed_commit,
        query=bug_title + '. ' + bug_description,
        top_K_results=top_K_results,
        field_to_return=["file_url", "source_code"]
    )

    return search_results


def search_result_ops(search_results):
    processed_results = []
    for result in search_results:
        file_url = result['file_url']
        source_code = result['source_code']
        bm25_score = result['bm25_score']

        try:
            json_result = java_py4j_ast_parser.processJavaFileContent(source_code)

            if json_result is None or json_result == '':
                # parse the source code if py4j fails
                try:
                    javaParser = JavaSourceParser(data=source_code)
                    parsed_methods = javaParser.parse_methods()
                except Exception as e:
                    print(f"Warning: Could not parse Java file {file_url} with JavaSourceParser: {e}")
                    # Skip this file if parsing fails
                    continue

            else:
                loaded_json = json.loads(json_result)
                parsed_methods = {}

                poly_morphism = 1
                # iterate over the parsed methods and get the method names and the method bodies
                for method in loaded_json:

                    method_name = method['member_name']
                    method_body = method['member_body']
                    class_name = method['class_name']

                    # clear the formatting of the method body for tokenization
                    method_body = clear_formatting(method_body)

                    # check if the method name is already in the parsed_methods
                    if method_name in parsed_methods:
                        # append the method body to the existing method name
                        parsed_methods[method_name + '!P' + str(
                            poly_morphism)] = 'Class: ' + class_name + ' \n Method: ' + method_body
                        poly_morphism += 1
                    else:
                        parsed_methods[method_name] = 'Class: ' + class_name + ' \n Method: ' + method_body

        except Exception as e:
            print(f"Warning: Could not process Java file {file_url}: {e}")
            # Skip this file if processing fails
            continue

        # create a json object with file_url and parsed_methods
        json_object = {
            'file_url': file_url,
            'methods': parsed_methods,
            'bm25_score': bm25_score
        }

        processed_results.append(json_object)

    return processed_results


def process_all_projects(base_dataset_path, output_base_path):
    """
    Process all projects from the ye et al dataset
    :param base_dataset_path: Base path containing all XML dataset files
    :param output_base_path: Base path for output files
    """
    print(f"Starting to process all projects from ye et al dataset")
    print(f"Base dataset path: {base_dataset_path}")
    print(f"Output base path: {output_base_path}")

    # Define the projects and their corresponding dataset files
    projects = {
        'aspectj': 'aspectj.xml',
        'eclipse': 'eclipse.xml',
        'birt': 'birt.xml',
        'swt': 'swt.xml',
        'jdt': 'jdt.xml',
        'tomcat': 'tomcat.xml'
    }

    total_successful_projects = 0
    total_failed_projects = 0

    for project_name, dataset_file in projects.items():
        print(f"\n{'=' * 50}")
        print(f"Processing project: {project_name}")
        print(f"{'=' * 50}")

        # Construct paths
        dataset_path = os.path.join(base_dataset_path, dataset_file)
        project_output_path = os.path.join(output_base_path, project_name)

        # Create output directory for this project
        os.makedirs(project_output_path, exist_ok=True)

        # Check if dataset file exists
        if not os.path.exists(dataset_path):
            print(f"Dataset file does not exist: {dataset_path}")
            print(f"Skipping project {project_name}")
            total_failed_projects += 1
            continue

        try:
            success = process_single_project(project_name, dataset_path, project_output_path)
            if success:
                total_successful_projects += 1
            else:
                total_failed_projects += 1
        except Exception as e:
            print(f"Error processing project {project_name}: {e}")
            total_failed_projects += 1
            continue

    print(f"\n{'=' * 50}")
    print(f"ALL PROJECTS PROCESSING COMPLETED!")
    print(f"{'=' * 50}")
    print(f"Successfully processed: {total_successful_projects} projects")
    print(f"Failed to process: {total_failed_projects} projects")
    print(f"Total projects processed: {total_successful_projects + total_failed_projects}")


def process_single_project(project_name, dataset_path, output_path):
    """
    Process a single project from the dataset
    :param project_name: Name of the project
    :param dataset_path: Path to the XML dataset file
    :param output_path: Path for output files
    """
    print(f"Processing project: {project_name}")
    print(f"Dataset path: {dataset_path}")
    print(f"Output path: {output_path}")

    # Parse the project's XML dataset
    bugs = parse_xml_dataset(dataset_path)

    # Add project name to each bug
    for bug in bugs:
        bug['project'] = project_name

    chunk_size = 100  # Smaller chunk size for XML dataset
    bugs_chunked = []

    # chunk the bugs up to chunk size
    for i in range(0, len(bugs), chunk_size):
        bugs_chunked.append(bugs[i:i + chunk_size])

    chunk_id = 1
    # iterate over the bugs_chunked
    for bug_chunk in tqdm(bugs_chunked, desc=f"Processing Bug Chunks for {project_name}"):
        # iterate over the bugs in each chunk
        for bug in tqdm(bug_chunk, desc=f"Processing Bugs for {project_name}"):
            bug_title = bug['bug_title']
            bug_description = bug['bug_description']
            project = bug['project']
            fixed_commit = bug['fixed_commit']

            # now search for the query in a method
            search_results = perform_search(project, fixed_commit, bug_title, bug_description, top_K_results=50)

            # now, perform ops in the search results
            processed_results = search_result_ops(search_results)

            # add processed results to the bug as a new key
            bug['es_results'] = processed_results

        # save the chunk to a file
        output_file = f"Cache_Res50_C{chunk_id}.json"
        JSON_File_IO.save_Dict_to_JSON(bug_chunk, output_path, output_file)
        chunk_id += 1

        # empty the bug_chunk from memory after saving to save memory
        bug_chunk = []

    print(f"Successfully processed {len(bugs)} bugs for project {project_name}")
    return True


gateway = JavaGateway()  # connect to the JVM
java_py4j_ast_parser = gateway.entry_point.getJavaMethodParser()  # get the HelloWorld instance

if __name__ == '__main__':
    # Process all projects from the dataset
    base_dataset_path = "/Users/armin/Desktop/UCI/bug-localization-project/Codes/Adjusted-BRaIn/Adjusted-BraIn/Data/ye et al"
    output_base_path = "cached_methods"
    process_all_projects(base_dataset_path, output_base_path)
