import os
import json
import csv
from collections.abc import MutableMapping

def read_json_files(json_file_paths):
    flattened_json_list = []
    for json_file_path in json_file_paths:
        read_json_file(json_file_path)
        json_content = read_json_file(json_file_path)
        flattened_json = flatten_json(json_content)
        flattened_json_list.append(flattened_json)
    return flattened_json_list


def read_json_file(json_file_path):
    with open(json_file_path, 'r') as f:
        json_contents = json.load(f)
    return json_contents

def flatten_json(json_dict, parent_key=False, separator='.'):
    items = []
    for key, value in json_dict.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten_json(value, new_key, separator).items())
        elif isinstance(value, list):
            for k, v in enumerate(value):
                items.extend(flatten_json({str(k): v}, new_key).items())
        else:
            items.append((new_key, value))
    return dict(items)

def write_json_output_to_csv(json_dicts_list, output_path):
    row_count = 0
    with open(output_path, "w", newline='') as myfile:
        csv_writer = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        for json_dict in json_dicts_list:
            if row_count == 0:
                header = json_dict.keys()
                csv_writer.writerow(header)
                row_count = row_count + 1
            csv_writer.writerow(json_dict.values())


def main():
    # your code here
    current_directory = os.getcwd()
    json_file_paths = get_json_file_paths(current_directory, r'data')
    flattened_json_contents = read_json_files(json_file_paths)
    output_csv_filepath = os.path.join(current_directory, "outfile.csv")
    write_json_output_to_csv(flattened_json_contents, output_csv_filepath)

def get_json_file_paths(current_directory, folder_name):
    json_file_paths = []
    data_directory = os.path.join(current_directory, folder_name)
    for root, _, files in os.walk(data_directory):
        for file_name in files:
            if "json" in file_name:
                json_file_paths.append(os.path.join(root, file_name))
    return json_file_paths
            



if __name__ == "__main__":
    main()
