import requests
import os


def download_data(local_file_path):
    if os.path.isfile(local_file_path):
        print(f"File '{local_file_path}' already exists. Skipping download.")
        return
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the local file in write-binary mode and write the content of the response to it
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")
