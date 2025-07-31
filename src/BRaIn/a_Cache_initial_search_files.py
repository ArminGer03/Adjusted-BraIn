import json
import xml.etree.ElementTree as ET
import re

from tqdm import tqdm

from src.IR import Searcher
from src.Utils import JavaSourceParser
from src.Utils.IO import JSON_File_IO
from src.Utils.Parser.JavaSourceParser import clear_formatting

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
        if 'bug_id' in bug and 'bug_title' in bug and 'bug_description' in bug:
            bugs.append(bug)
    
    print(f"Parsed {len(bugs)} bugs from XML dataset")
    return bugs

def perform_search(project, bug_title, bug_description, top_K_results=10):
    searcher = Searcher('ye_et_al')  # Use the ye_et_al index
    search_results = searcher.search_Extended(
        project=project,
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
                        parsed_methods[method_name+'!P'+str(poly_morphism)] = 'Class: '+ class_name + ' \n Method: ' + method_body
                        poly_morphism += 1
                    else:
                        parsed_methods[method_name] = 'Class: '+ class_name + ' \n Method: ' + method_body

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

from py4j.java_gateway import JavaGateway

gateway = JavaGateway()  # connect to the JVM
java_py4j_ast_parser = gateway.entry_point.getJavaMethodParser()  # get the HelloWorld instance

if __name__ == '__main__':
    # Parse the aspectj XML dataset
    xml_path = "../../Data/ye et al/aspectj.xml"
    bugs = parse_xml_dataset(xml_path)
    
    # Set project name for all bugs
    project_name = "aspectj"
    
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
    for bug_chunk in tqdm(bugs_chunked, desc="Processing Bug Chunks"):
        # iterate over the bugs in each chunk
        for bug in tqdm(bug_chunk, desc="Processing Bugs"):
            bug_title = bug['bug_title']
            bug_description = bug['bug_description']
            project = bug['project']

            # now search for the query in a method
            search_results = perform_search(project, bug_title, bug_description, top_K_results=50)

            # now, perform ops in the search results
            processed_results = search_result_ops(search_results)

            # add processed results to the bug as a new key
            bug['es_results'] = processed_results

        # save the chunk to a file
        json_save_path = "cached_methods"
        #use chunk_id to save the file
        JSON_File_IO.save_Dict_to_JSON(bug_chunk, json_save_path, f"Cache_Res50_C{chunk_id}.json")
        chunk_id += 1

        # empty the bug_chunk from memory after saving to save memory
        bug_chunk = []

