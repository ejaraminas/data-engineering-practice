import os
import zipfile
import aiohttp
import asyncio

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def add_directory(dir_name):
    """Create new downloads directory"""
    if os.path.isdir(dir_name):
        print("Folder exists. Continuing.")
    else:
        os.mkdir(dir_name)
        print("Folder created. Continuing.")
    os.chdir(dir_name)


async def download_file(uri):
    """Use async download to retrieve file"""
    file_name = uri.rsplit("/")[3]
    print(f"Retrieving: {file_name}", end="")
    async with aiohttp.ClientSession() as session:
        async with session.get(uri) as response:
            if response.status == 200:
                with open(file_name, 'wb') as f:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
            else:
                print(f" - Failed to download. Status code: {response.status}", end="")
    print("--> Done.")


async def main():

    add_directory("downloads")

    """Attempt downloading each zip"""
    for uri in download_uris:
        await download_file(uri)

    """Extract CSV file from each zip file"""
    zip_list = os.listdir()

    for zip_file in zip_list:
        zf = zipfile.ZipFile(zip_file, 'r')
        zf_file_list = zf.namelist()
        for file in zf_file_list:
            if ".csv" in file:
                zf.extract(file)
        os.remove(zip_file)


if __name__ == "__main__":
    asyncio.run(main())
