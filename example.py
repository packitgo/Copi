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
        
        self.site_name = kwargs.get('site_name', 'default_site')
        self.category = kwargs.get('category', 'default_category')
        today = datetime.now().strftime("%Y-%m-%d")
        formatted_category = self.category.replace(" ", "_")
        
        log_dir = os.path.join('C:\\Users\\inyur\\ScraperProject\\logs')
        log_file = os.path.join(log_dir, f'{self.site_name}_{formatted_category}_{today}.log')
        
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        logging.basicConfig(filename=log_file, level=logging.DEBUG, 
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levellevel)s - %(message)s'))
        self.my_logger = logging.getLogger(__name__)
        self.my_logger.addHandler(console_handler)

        driver_path = os.path.join('C:\\Users\\inyur\\Favorites\\geckodriver.exe')
        
        if not os.path.exists(driver_path):
            self.my_logger.error(f"Geckodriver not found at {driver_path}")
            raise FileNotFoundError(f"Geckodriver not found at {driver_path}")
        
        self.my_logger.info(f"Using geckodriver at: {driver_path}")
        self.driver = webdriver.Firefox(service=Service(driver_path))
        
        results_base_dir = os.path.join('C:\\Users\\inyur\\ScraperProject\\results')
        self.results_dir = os.path.join(results_base_dir, self.site_name, formatted_category)
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)
            self.my_logger.info(f"Created results directory: {self.results_dir}")
        else:
            self.my_logger.info(f"Results directory already exists: {self.results_dir}")

        self.excel_file = os.path.join(self.results_dir, f'{self.site_name}_{formatted_category}_{today}.xlsx')
        self.my_logger.info(f"Excel file will be saved at: {self.excel_file}")
        self.init_excel()

        self.delay = 10
        self.max_delay = 30
        self.delay_step = 2

    def init_excel(self):
        if not os.path.exists(self.excel_file):
            try:
                df = pd.DataFrame(columns=[
                    'No', 'URL', 'product_name', 'price', 'description', 
                    'main_image_1', 'main_image_2', 'main_image_3',
                    'description_images_1', 'description_images_2',
                    'product_code', 'size', 'tip', 'thickness', 'material', 
                    'color', 'quantity_in_box', 'URL type'
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
        links_file = os.path.join('C:\\Users\\inyur\\ScraperProject\\data', self.site_name, formatted_category, 'results.csv')
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

        time.sleep(self.delay + random.uniform(0, 2))
        if self.is_blocked(response):
            self.delay = min(self.delay + self.delay_step, self.max_delay)
            self.my_logger.warning(f"Possible blocking, increasing delay to {self.delay} seconds.")
        else:
            self.delay = max(self.delay - self.delay_step, 1)
            self.my_logger.debug(f"Decreasing delay to {self.delay} seconds.")

        try:
            self.driver.implicitly_wait(10)
            product_data = self.extract_product_data()
            self.update_excel(response.url, product_data, response.meta['url_type'])
        except Exception as e:
            self.my_logger.error(f"Error processing URL {response.url}: {e}")

    def extract_product_data(self):
        product_datafields = [
            ('product_name', self.get_product_name),
            ('price', self.get_price),
            ('description', self.get_description),
            ('main_image_1', self.get_main_image_1),
            ('main_image_2', self.get_main_image_2),
            ('main_image_3', self.get_main_image_3),
            ('description_images_1', self.get_description_images_1),
            ('description_images_2', self.get_description_images_2),
            ('product_code', self.get_product_code),
            ('size', self.get_size),
            ('tip', self.get_tip),
            ('thickness', self.get_thickness),
            ('material', self.get_material),
            ('color', self.get_color),
            ('quantity_in_box', self.get_quantity_in_box)
        ]

        product_data = {}
        for field, method in product_datafields:
            try:
                product_data[field] = method()
            except Exception as e:
                self.my_logger.error(f"Error extracting {field}: {e}")
                product_data[field] = None

        return product_data

    def get_product_name(self):
        try:
            return self.driver.find_element(By.CSS_SELECTOR, 'div.product-name').text.strip()
        except Exception as e:
            self.my_logger.error(f"Product name not found: {e}")
            return None

    def get_price(self):
        try:
            return self.driver.find_element(By.CSS_SELECTOR, 'span.price').text.strip()
        except Exception as e:
            self.my_logger.error(f"Price not found: {e}")
            return None

    def get_description(self):
        try:
            return self.driver.find_element(By.CSS_SELECTOR, 'div.description').text.strip()
        except Exception as e:
            self.my_logger.error(f"Description not found: {e}")
            return None

    def get_main_image_1(self):
        try:
            return self.driver.find_element(By.CSS_SELECTOR, 'img.main-image').get_attribute('src')
        except Exception as e:
            self.my_logger.error(f"Main image 1 not found: {e}")
            return None

    def get_main_image_2(self):
        try:
            images = self.driver.find_elements(By.CSS_SELECTOR, 'img.additional-image')
            return images[0].get_attribute('src') if len(images) > 0 else None
        except Exception as e:
            self.my_logger.error(f"Main image 2 not found: {e}")
            return None

    def get_main_image_3(self):
        try:
            images = self.driver.find_elements(By.CSS_SELECTOR, 'img.additional-image')
            return images[1].get_attribute('src') if len(images) > 1 else None
        except Exception as e:
            self.my_logger.error(f"Main image 3 not found: {e}")
            return None

    def get_description_images_1(self):
        try:
            return [img.get_attribute('src') for img in self.driver.find_elements(By.CSS_SELECTOR, 'div.description-images img')]
        except Exception as e:
            self.my_logger.error(f"Description images 1 not found: {e}")
            return []

    def get_description_images_2(self):
        try:
            return [img.get_attribute('src') for img in self.driver.find_elements(By.CSS_SELECTOR, 'div.additional-description-images img')]
        except Exception as e:
            self.my_logger.error(f"Description images 2 not found: {e}")
            return []

    def get_product_code(self):
        try:
            return self.driver.find_element(By.CSS_SELECTOR, 'span.product-code').text.strip()
        except Exception as e:
            self.my_logger.error(f"Product code not found: {e}")
            return None

    def get_size(self):
        try:
            return self.driver.find_element(By.XPATH, '//div[contains(text(), "Size")]/following-sibling::div').text.strip()
        except Exception as e:
            self.my_logger.error(f"Size not found: {e}")
            return None

    def get_tip(self):
        try:
            return self.driver.find_element(By.CSS_SELECTOR, 'div.tip').text.strip()
        except Exception as e:
            self.my_logger.error(f"Tip not found: {e}")
            return None

    def get_thickness(self):
        try:
            return self.driver.find_element(By.XPATH, '//div[contains(text(), "Thickness")]/following-sibling::div').text.strip()
        except Exception as e:
            self.my_logger.error(f"Thickness not found: {e}")
            return None

    def get_material(self):
        try:
            return self.driver.find_element(By.XPATH, '//div[contains(text(), "Material")]/following-sibling::div').text.strip()
        except Exception as e:
            self.my_logger.error(f"Material not found: {e}")
            return None

    def get_color(self):
        try:
            return self.driver.find_element(By.XPATH, '//div[contains(text(), "Color")]/following-sibling::div').text.strip()
        except Exception as e:
            self.my_logger.error(f"Color not found: {e}")
            return None

    def get_quantity_in_box(self):
        try:
            return self.driver.find_element(By.XPATH, '//div[contains(text(), "Quantity")]/following-sibling::div').text.strip()
        except Exception as e:
            self.my_logger.error(f"Quantity in box not found: {e}")
            return None

    def update_excel(self, url, product_data, url_type):
        try:
            df = pd.read_excel(self.excel_file)
            if url in df['URL'].values:
                index = df.index[df['URL'] == url].tolist()[0]
                for key, value in product_data.items():
                    df.at[index, key] = value if value is not None else df.at[index, key]
                df.at[index, 'URL type'] = url_type
            else:
                new_row = {'URL': url, 'URL type': url_type, **product_data}
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            df.to_excel(self.excel_file, index=False)
            self.my_logger.info(f"Data saved to Excel for URL: {url}")
        except Exception as e:
            self.my_logger.error(f"Error updating Excel file: {e}")

    def is_blocked(self, response):
        return response.status in [403, 503]

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