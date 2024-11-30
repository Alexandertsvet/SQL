import csv
import pathlib
import requests


class ImageExtractPipeline:

    def process_item(self, item, spider):
        file_path_csv = pathlib.Path(__file__).parent / 'data' / 'csv' / 'scv_image.csv'
        file_path_image = pathlib.Path(__file__).parent / 'data' / 'image'
        file_path_image.parent.mkdir(exist_ok=True, parents=True)
        file_path_csv.parent.mkdir(exist_ok=True, parents=True)
        with open(file_path_csv, 'a', newline='') as csvfile:
                fieldnames = ['name', 'category', 'link']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for key, value in item.items():
                    writer.writerow({'name': key, 'category': value['category'], 'link': value['link']})
                    response = requests.get(value['link'], stream=True)
                    file_path_image_file = file_path_image / f'{key}'
                    file_path_image_file.parent.mkdir(exist_ok=True, parents=True)

                    if response.status_code == 200:
                        with open(file_path_image_file, 'wb') as file:
                            for chunk in response.iter_content(1024):
                                file.write(chunk)
        return item
