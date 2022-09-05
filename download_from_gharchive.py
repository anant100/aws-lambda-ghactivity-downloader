import requests

def download_file(file_name):
    res = requests.get(f'https://data.gharchive.org/{file_name}')
    return res