import boto3
from pathlib import Path
import os
import gzip
def download_s3_file(bucket_name, file_key):
    s3 = boto3.client('s3', region_name="us-east-1")
    file_name = file_key.split("/")[-1]
    print(f"Downloading {file_name} from {bucket_name}")
    s3.download_file(bucket_name, file_key, f"downloads/{file_name}")
    return file_name

def extract_wetpaths_file_and_return_first_line(wetpaths_directory_path):
    with gzip.open(wetpaths_directory_path, 'rb') as f:
       first_line = f.readline()
       first_line = first_line.decode("utf-8")
       first_line = first_line.replace("\n", "")
       return first_line

def extract_gzip_and_print_contents(directory):
     with gzip.open(directory, 'rb') as f:
         for line in f:
             line = line.decode("utf-8")
             print(line)

def create_downloads_folder():
    current_directory = os.getcwd()
    final_directory = os.path.join(current_directory, r'downloads')
    Path(final_directory).mkdir(parents=True, exist_ok=True)
    return final_directory

def main():
    # your code here
    bucket_name = "commoncrawl"
    file_key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    directory_path = create_downloads_folder()
    file_name = download_s3_file(bucket_name, file_key)
    second_file_key = extract_wetpaths_file_and_return_first_line(f"{directory_path}/{file_name}")
    second_file_name = download_s3_file(bucket_name, second_file_key)
    extract_gzip_and_print_contents(f"{directory_path}/{second_file_name}")

    


if __name__ == "__main__":
    main()
