import asyncio
import aiohttp
from pathlib import Path
import os
import zipfile
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
]

async def download_file(arguments):
    directory = arguments[0]
    url = arguments[1]
    print(f"Calling download_file with arguments {directory} {url}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if "content-disposition" in response.headers:
                content_disposition = response.headers["content-disposition"]
                filename = content_disposition.split("filename=")[1]
            else:
                filename = url.split("/")[-1]
            zip_directory_path = os.path.join(directory,filename)
            with open(zip_directory_path, "wb") as file:
                while True:
                    chunk = await response.content.read()
                    if not chunk:
                        break
                    file.write(chunk)
                with zipfile.ZipFile(zip_directory_path, 'r') as zip_ref:
                    zip_ref.extractall(directory)
            os.remove(zip_directory_path)
            print(f"Downloaded file: {filename}")
         


async def main():
    # your code here
    current_directory = os.getcwd()
    final_directory = os.path.join(current_directory, r'downloads')
    Path(final_directory).mkdir(parents=True, exist_ok=True)
    download_file_tasks = [download_file((final_directory, url)) for url in download_uris]
    await asyncio.gather(*download_file_tasks)


if __name__ == "__main__":
      asyncio.run(main())