
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from tqdm import tqdm

from functions import make_dir, save_csv

query_params = str(input('введение запрос для поиска изображения: '))
img_count = int(input('введение количество изображения для скачивания: '))

options = Options()
options.add_experimental_option("detach", True)
options.add_argument('start_maximized')
driver = webdriver.Chrome(options=options)
driver.get("https://unsplash.com/")

driver.implicitly_wait(5)


input = driver.find_element(By.XPATH, "//input")
input.send_keys(query_params)
input.send_keys(Keys.ENTER)

weit_buttom_load_more = WebDriverWait(driver, timeout=5)
buttom_load_more = weit_buttom_load_more.until(
    expected_conditions.presence_of_element_located(
        (By.XPATH, "//button[@class='ZGh7S kx6eK x_EXo R6ToQ QcIGU l0vpf a_AEd ncszm MCje9 daPLj R6ToQ']")
        ))
buttom_load_more.click()
element_len = len(driver.find_elements(By.XPATH, "//img[@class='I7OuT DVW3V L1BOa']"))
if element_len > img_count:
    elements = driver.find_elements(By.XPATH, "//img[@class='I7OuT DVW3V L1BOa']")[:img_count]
else:
    while element_len < img_count:
        driver.implicitly_wait(1)
        driver.execute_script("window.scrollBy(0, 5000)")
        elements = driver.find_elements(By.XPATH, "//img[@class='I7OuT DVW3V L1BOa']")
        element_len = len(elements)
        element_len = element_len

file_path_csv, file_path_image = make_dir()
for element in tqdm(elements):
    name = element.get_attribute('alt')
    image_link = element.get_attribute('src')
    result = {'name': str(name),
              'image_link': str(image_link)}
    save_csv(file_path_csv, file_path_image, **result)

driver.quit()
