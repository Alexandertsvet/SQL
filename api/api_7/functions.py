import csv
import pathlib
import requests


def make_dir():
    file_path_csv = pathlib.Path(__file__).parent / 'data' / 'csv' / 'scv_image.csv'
    file_path_image = pathlib.Path(__file__).parent / 'data' / 'image'
    file_path_image.parent.mkdir(exist_ok=True, parents=True)
    file_path_csv.parent.mkdir(exist_ok=True, parents=True)
    return (file_path_csv, file_path_image)


def save_csv(file_path_csv, file_path_image, **kwargs):
    with open(file_path_csv, 'a', newline='') as csvfile:
                fieldnames = ['name', 'image_link']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                name = kwargs.get('name')
                image_link = kwargs.get('image_link')
                if not name:
                    name = 'unknown'
                if not image_link:
                    image_link = 'unknown'
                writer.writerow({'name': name, 'image_link': image_link})
                file_path_image_file = file_path_image / f'{name}'
                file_path_image_file.parent.mkdir(exist_ok=True, parents=True)
                response = requests.get(kwargs['image_link'], stream=True)
                if response.status_code == 200:
                    with open(file_path_image_file, 'wb') as file:
                        for chunk in response.iter_content(1024):
                            file.write(chunk)
                return None
