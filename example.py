import scrapy
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
import os
from datetime import datetime
import logging
import time
import random

class ExampleSpider(scrapy.Spider):
    name = "example"

    def __init__(self, *args, **kwargs):
        super(ExampleSpider, self).__init__(*args, **kwargs)
        
        # Настройка логирования
        self.site_name = kwargs.get('site_name', 'default_site')
        self.category = kwargs.get('category', 'default_category')
        today = datetime.now().strftime("%Y-%m-%d")
        formatted_category = self.category.replace(" ", "_")
        log_file = os.path.join('C:/Users/inyur/ScraperProject/logs', f'{self.site_name}_{formatted_category}_{today}.log')
        logging.basicConfig(filename=log_file, level=logging.DEBUG, 
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.my_logger = logging.getLogger(__name__)
        self.my_logger.addHandler(console_handler)

        self.driver = webdriver.Firefox(service=Service('C:/Users/inyur/Favorites/geckodriver.exe'))
        self.results_dir = os.path.join('C:/Users/inyur/ScraperProject/results', self.site_name, formatted_category)
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)
            self.my_logger.info(f"Created results directory: {self.results_dir}")
        else:
            self.my_logger.info(f"Results directory already exists: {self.results_dir}")

        self.excel_file = os.path.join(self.results_dir, f'{self.site_name}_{formatted_category}_{today}.xlsx')
        self.my_logger.info(f"Excel file will be saved at: {self.excel_file}")
        self.init_excel()

        # Динамическая регулировка задержек
        self.delay = 10  # Начальная задержка в секундах
        self.max_delay = 30  # Максимальная задержка
        self.delay_step = 2  # Шаг изменения задержки

    def init_excel(self):
        if not os.path.exists(self.excel_file):
            try:
                df = pd.DataFrame(columns=[
                    'No', 'URL', 'product_name', 'price', 'description', 'main_image',
                    'other_pictures', 'description_images', 'product_code', 'size',
                    'tip', 'thickness', 'material', 'color', 'quantity_in_box', 'URL type'
                ])
                df.to_excel(self.excel_file, index=False)
                self.my_logger.info(f"Created new Excel file: {self.excel_file}")
            except Exception as e:
                self.my_logger.error(f"Error creating Excel file: {e}")
                raise
        else:
            self.my_logger.info(f"Excel file already exists: {self.excel_file}")

    def start_requests(self):
        self.my_logger.info("Starting spider requests")
        formatted_category = self.category.replace(" ", "_")
        links_file = os.path.join('C:/Users/inyur/ScraperProject/data', self.site_name, formatted_category, 'results.csv')
        self.my_logger.info(f"Checking for links file at: {links_file}")
        if os.path.exists(links_file):
            self.my_logger.info(f"Found links file: {links_file}")
            try:
                df = pd.read_csv(links_file)
                self.my_logger.info(f"Successfully read CSV file: {links_file} with {len(df)} rows")
                for index, row in df.iterrows():
                    self.my_logger.info(f"Processing row {index}: {row}")
                    url = row['URL']
                    url_type = row.get('URL type', 'New')
                    yield scrapy.Request(url=url, callback=self.parse, meta={'url_type': url_type})
            except Exception as e:
                self.my_logger.error(f"Error reading links file {links_file}: {e}")
        else:
            self.my_logger.error(f"Links file {links_file} not found.")

    def parse(self, response):
        self.my_logger.info(f"Processing URL: {response.url}")
        self.driver.get(response.url)

        # Проверка на признаки блокировки и регулировка задержки
        if self.is_blocked(response):
            self.delay = min(self.delay + self.delay_step, self.max_delay)
            self.my_logger.warning(f"Признаки блокировки, увеличение задержки до {self.delay} секунд.")
        else:
            self.delay = max(self.delay - self.delay_step, 1)
            self.my_logger.debug(f"Уменьшение задержки до {self.delay} секунд.")

        # Случайная задержка перед следующим запросом
        time.sleep(self.delay + random.uniform(0, 2))

        # Проверка на загрузку основных элементов
        try:
            self.driver.implicitly_wait(10)
            product_name_element = self.driver.find_element(By.CSS_SELECTOR, 'div.options')
        except Exception as e:
            self.my_logger.error(f"Main elements not loaded for URL: {response.url} - {e}")
            return

        # Извлечение данных
        product_name = price = description = main_image = product_code = size = tip = thickness = material = color = quantity_in_box = None
        other_pictures = description_images = []

        try:
            product_name = product_name_element.text.strip()
        except Exception as e:
            self.my_logger.error(f"Error extracting product name: {e}")

        try:
            price_element = self.driver.find_element(By.CSS_SELECTOR, 'span.p.opensans')
            price = price_element.text.strip()
        except Exception as e:
            self.my_logger.error(f"Error extracting price: {e}")

        try:
            description_element = self.driver.find_element(By.CSS_SELECTOR, 'div.product_content_desc')
            description = description_element.text.strip()
        except Exception as e:
            self.my_logger.error(f"Error extracting description: {e}")

        try:
            main_image_element = self.driver.find_element(By.CSS_SELECTOR, 'img[src*="/shopimg_new/"]')
            main_image = main_image_element.get_attribute('src')
        except Exception as e:
            self.my_logger.error(f"Error extracting main image: {e}")

        try:
            other_pictures_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.swiper-slide img')
            other_pictures = [img.get_attribute('src') for img in other_pictures_elements]
        except Exception as e:
            self.my_logger.error(f"Error extracting other pictures: {e}")

        try:
            description_images_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.add_contents img')
            description_images = [img.get_attribute('src') for img in description_images_elements]
        except Exception as e:
            self.my_logger.error(f"Error extracting description images: {e}")

        try:
            product_code_element = self.driver.find_element(By.CSS_SELECTOR, 'div.product_content_desc span.desc_col_text')
            product_code = product_code_element.text.strip()
        except Exception as e:
            self.my_logger.error(f"Error extracting product code: {e}")

        try:
            size_element = self.driver.find_element(By.XPATH, '//div[contains(text(), "크기")]/following-sibling::div')
            size = size_element.text.strip()
        except Exception as e:
            self.my_logger.error(f"Error extracting size: {e}")

        try:
            tip_element = self.driver.find_element(By.CSS_SELECTOR, 'div.info_row div.text')
            tip = tip_element.text.strip()
        except Exception as e:
            self.my_logger.error(f"Error extracting tip: {e}")

        try:
            thickness_element = self.driver.find_element(By.XPATH, '//div[contains(text(), "두께")]/following-sibling::div')
            thickness = thickness_element.text.strip()
        except Exception as e:
            self.my_logger.error(f"Error extracting thickness: {e}")

        try:
            material_element = self.driver.find_element(By.XPATH, '//div[contains(text(), "재질")]/following-sibling::div')
            material = material_element.text.strip()
        except Exception as e:
            self.my_logger.error(f"Error extracting material: {e}")

        try:
            color_element = self.driver.find_element(By.XPATH, '//div[contains(text(), "색상")]/following-sibling::div')
            color = color_element.text.strip()
        except Exception as e:
            self.my_logger.error(f"Error extracting color: {e}")

        try:
            quantity_in_box_element = self.driver.find_element(By.XPATH, '//div[contains(text(), "포장단위")]/following-sibling::div')
            quantity_in_box = quantity_in_box_element.text.strip()
        except Exception as e:
            self.my_logger.error(f"Error extracting quantity in box: {e}")

        self.update_excel(response.url, product_name, price, description, main_image,
                          other_pictures, description_images, product_code, size,
                          tip, thickness, material, color, quantity_in_box, response.meta['url_type'])

    def update_excel(self, url, product_name, price, description, main_image,
                      other_pictures, description_images, product_code, size,
                      tip, thickness, material, color, quantity_in_box, url_type):
        try:
            df = pd.read_excel(self.excel_file)
            if url in df['URL'].values:
                index = df.index[df['URL'] == url].tolist()[0]
                df.loc[index] = [url, product_name, price, description, main_image,
                                 ','.join(other_pictures), ','.join(description_images), product_code, size,
                                 tip, thickness, material, color, quantity_in_box, url_type]
            else:
                new_row = {
                    'URL': url, 'product_name': product_name, 'price': price,
                    'description': description, 'main_image': main_image,
                    'other_pictures': ','.join(other_pictures), 'description_images': ','.join(description_images),
                    'product_code': product_code, 'size': size, 'tip': tip,
                    'thickness': thickness, 'material': material, 'color': color,
                    'quantity_in_box': quantity_in_box, 'URL type': url_type
                }
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            df.to_excel(self.excel_file, index=False)
            self.my_logger.info(f"Data saved to Excel for URL: {url}")
        except Exception as e:
            self.my_logger.error(f"Error updating Excel file: {e}")

    def is_blocked(self, response):
        # Проверка на коды ошибок HTTP
        if response.status in [403, 503]:
            return True

        # Проверка на CAPTCHA (добавьте свою логику)
        # ...

        # Проверка на пустые ответы (добавьте свою логику)
        # ...

        return False

    def closed(self, reason):
        self.driver.quit()
        self.my_logger.info(f"Spider {self.name} closed because: {reason}")

if __name__ == "__main__":
    from scrapy.crawler import CrawlerProcess
    import sys

    site_name = sys.argv[1]
    category = sys.argv[2]

    process = CrawlerProcess({
        'LOG_LEVEL': 'DEBUG',
    })
    process.crawl(ExampleSpider, site_name=site_name, category=category)
    process.start()
