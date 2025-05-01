import sys
import os
import traceback
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import logging.handlers
import time
import uuid
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import boto3
import requests
import uvicorn
from bs4 import BeautifulSoup
from fastapi import FastAPI, BackgroundTasks
from selenium import webdriver
from selenium.webdriver.common.by import By
import csv
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import datetime

# Validate required environment variables
required_env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'SETTINGS_URL']
for var in required_env_vars:
    if not os.getenv(var):
        logging.error(f"Missing required environment variable: {var}")
        raise EnvironmentError(f"Environment variable {var} is not set")


app = FastAPI(title="agent_brand_api_scraper", version="2.0.1")

@app.post("/run_parser")
async def brand_batch_endpoint(job_id: str, brand_id: str, send_out_endpoint_local: str, background_tasks: BackgroundTasks):
    global send_out_endpoint
    send_out_endpoint = send_out_endpoint_local
    background_tasks.add_task(run_parser, job_id, brand_id)
    return {"message": "Parser task started in the background"}

def fetch_settings():
    try:
        response = requests.get(os.getenv('SETTINGS_URL'))
        response.raise_for_status()
        logging.info(f"Settings file fetched successfully")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch settings: {traceback.format_exc()}")
        return None

def run_parser(job_id, brand_id):
    logging.info(f"Running parser for brand_id: {brand_id}, job_id: {job_id}")
    settings = fetch_settings()
    if not settings or brand_id not in settings:
        logging.error(f"No settings found for brand_id: {brand_id}")
        return

    brand_settings = settings.get(brand_id)
    categories = brand_settings.get("Categories")
    locales = brand_settings.get("Locales")
    base_url = brand_settings.get("Base_URL")
    logging.info(f"Parser parameters: Base URL: {base_url}, Categories: {categories}, Locales: {locales}")

    parsers = {
        '478': (SaintLaurentProductParser, categories, locales),
        '229': (GucciProductParser, categories, locales),
        '26': (AlexanderMcqueenParser, categories, locales),
        '363': (MonclerProductParser, categories, locales),
        '314': (LoroPianaProductParser, categories, locales),
        '310': (LoeweProductParser, categories, locales),
        '157': (DolceGabbanaProductParser, categories, locales),
        '500': (StoneIslandProductParser, categories, locales),
        '125': (ChloeProductParser, categories, locales),
        '110': (CanadaGooseProductParser, categories, locales)
    }

    if brand_id in parsers:
        parser_class, categories, locales = parsers[brand_id]
        parser = parser_class(job_id, base_url)
        parser.process_categories(categories, locales)
    else:
        logging.error(f"No parser defined for brand_id: {brand_id}")
    logging.info("Parser execution completed")

class WebsiteParser:
    def __init__(self, job_id, brand):
        self.output_filename = None
        self.upload_url = None
        self.count = 0
        self.log_url = None
        self.session = requests.Session()
        self.code = str(uuid.uuid4())
        self.job_id = job_id
        self.brand = brand
        self.setup_logging()

    def setup_logging(self):
        current_date = datetime.datetime.now().strftime("%d_%m_%Y")
        self.log_file_name = f'{self.brand}_{self.code}_{current_date}.log'
        logger_name = f"Brand Name: {self.brand}, Job ID: {self.job_id}, UUID: {self.code}"
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler = logging.handlers.RotatingFileHandler(self.log_file_name)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.logger.debug("Logger initialized for debugging")
        self.logger.info("Logger initialized for console output")

    def format_url(self, url):
        if url:
            return url if url.startswith('http') else 'https:' + url
        return ""

    def safe_strip(self, value):
        return value.strip() if isinstance(value, str) else value

    def convert_to_tsv(self, data):
        return [[str(item) for item in row] for row in data]

    def write_to_csv(self, csv_data):
        current_date = datetime.datetime.now().strftime("%d_%m_%Y")
        file_path = f'output_{self.brand}_{self.code}_{current_date}.csv'
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerows(csv_data)
        self.logger.info(f"Data saved to '{file_path}'")
        return file_path

    def get_s3_client(self):
        self.logger.info("Creating S3 client")
        try:
            region = os.getenv('AWS_REGION', 'us-east-2')
            session = boto3.session.Session()
            client = session.client(
                service_name='s3',
                region_name=region,
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )
            self.logger.info("S3 client created successfully")
            return client
        except Exception:
            self.logger.error(f"Error creating S3 client: {traceback.format_exc()}")
            return None

    def upload_file_to_space(self, file_src, save_as, is_public=True):
        self.logger.info(f"Uploading file to space: {save_as}\nFrom: {file_src}")
        spaces_client = self.get_s3_client()
        if not spaces_client:
            self.logger.error("S3 client creation failed. Cannot upload file.")
            return None
        space_name = 'archive.iconluxurygroup'
        try:
            spaces_client.upload_file(
                str(file_src),
                space_name,
                str(save_as),
                ExtraArgs={'ACL': 'public-read'} if is_public else None
            )
            self.logger.info(f"File uploaded successfully to {space_name}/{save_as}")
            if is_public:
                upload_url = f"https://{space_name}.s3.us-east-2.amazonaws.com/{save_as}"
                self.logger.info(f"Public URL: {upload_url}")
                return upload_url
        except Exception:
            self.logger.error(f"Error uploading file to space: {traceback.format_exc()}")
            return None

    def send_output(self):
        self.logger.info("Starting send output")
        self.log_url = self.upload_file_to_space(self.log_file_name, self.log_file_name)
        logging.shutdown()
        headers = {
            'accept': 'application/json',
            'content-type': 'application/x-www-form-urlencoded',
        }
        params = {
            'job_id': str(self.job_id),
            'resultUrl': str(self.upload_url),
            'logUrl': str(self.log_url),
            'count': int(self.count)
        }
        try:
            if os.path.exists(self.output_filename):
                os.remove(self.output_filename)
                self.logger.info(f"Removed output file: {self.output_filename}")
            if os.path.exists(self.log_file_name):
                os.remove(self.log_file_name)
                self.logger.info(f"Removed log file: {self.log_file_name}")
        except Exception:
            self.logger.error(f"Error cleaning up files: {traceback.format_exc()}")
        self.logger.info(f"Finishing send output: {params}")
        try:
            response = requests.post(f"{send_out_endpoint}/job_complete", params=params, headers=headers)
            response.raise_for_status()
            self.logger.info("Successfully sent job completion notification")
        except requests.RequestException:
            self.logger.error(f"Error sending job completion notification: {traceback.format_exc()}")

    @staticmethod
    def open_link(url):
        try:
            session = requests.Session()
            retries = Retry(
                total=5,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS"]
            )
            session.mount("https://", HTTPAdapter(max_retries=retries))
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.3"
            }
            response = session.get(url, headers=headers, allow_redirects=True)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logging.error(f"Error opening URL {url}: {traceback.format_exc()}")
            return None

class GucciProductParser(WebsiteParser):
    def __init__(self, job_id, base_url):
        super().__init__(job_id, 'gucci')
        self.data = pd.DataFrame()
        self.base_url = base_url
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
        options.add_argument("--start-maximized")
        options.add_argument("--incognito")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-setuid-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        self.driver = webdriver.Chrome(options=options)

    def fetch_data(self, category, base_url, locale):
        all_products = []
        try:
            current_url = base_url.format(category=category, page=0, locale=locale)
            self.driver.get(current_url)
            page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
            self.logger.debug(f"Page source (first 10000 chars): {page_source[:10000]}")
            soup = BeautifulSoup(page_source, 'html.parser')
            json_temp = soup.find('pre').text if soup.find('pre') else ''
            json_data = json.loads(json_temp) if json_temp else {}
            total_pages = json_data.get('numberOfPages', 1)
            self.logger.info(f"Category: {category}, Total Pages: {total_pages}")

            for page in range(total_pages):
                current_url = base_url.format(category=category, page=page, locale=locale)
                self.driver.get(current_url)
                page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
                soup = BeautifulSoup(page_source, 'html.parser')
                json_temp = soup.find('pre').text if soup.find('pre') else ''
                json_data = json.loads(json_temp) if json_temp else {}
                items = json_data.get('products', {}).get('items', [])
                if not items:
                    self.logger.info(f"No items found on Page: {page + 1}/{total_pages} URL: {current_url}")
                    continue

                for product in items:
                    self.logger.debug(f"Parsing product JSON: {product}")
                    product_info = {
                        'category': category,
                        'productCode': self.safe_strip(product.get('productCode', '')),
                        'title': self.safe_strip(product.get('title', '')).replace('\n', ' ').replace('\r', ''),
                        'price': self.safe_strip(product.get('price', '')),
                        'rawPrice': self.safe_strip(product.get('rawPrice', '')),
                        'productLink': self.safe_strip(product.get('productLink', '')),
                        'primaryImage': self.format_url(product.get('primaryImage', {}).get('src', '')),
                        'alternateGalleryImages': " | ".join([self.format_url(img.get('src', '')) for img in product.get('alternateGalleryImages', [])]),
                        'alternateImage': self.format_url(product.get('alternateImage', {}).get('src', '')),
                        'isFavorite': str(product.get('isFavorite', False)).lower(),
                        'isOnlineExclusive': str(product.get('isOnlineExclusive', False)).lower(),
                        'isRegionalOnlineExclusive': str(product.get('isRegionalOnlineExclusive', False)).lower(),
                        'regionalOnlineExclusiveMsg': self.safe_strip(product.get('regionalOnlineExclusiveMsg', '')),
                        'isExclusiveSale': str(product.get('isExclusiveSale', False)).lower(),
                        'label': self.safe_strip(product.get('label', '')),
                        'fullPrice': self.safe_strip(product.get('fullPrice', '')),
                        'position': int(product.get('position', 0)),
                        'productName': self.safe_strip(product.get('productName', '')),
                        'showSavedItemIcon': str(product.get('showSavedItemIcon', False)).lower(),
                        'type': self.safe_strip(product.get('type', '')),
                        'saleType': self.safe_strip(product.get('saleType', '')),
                        'categoryPath': self.safe_strip(product.get('categoryPath', '')),
                        'variant': self.safe_strip(product.get('variant', '')),
                        'videoBackgroundImage': self.format_url(product.get('videoBackgroundImage', '')),
                        'zoomImagePrimary': self.format_url(product.get('zoomImagePrimary', '')),
                        'zoomImageAlternate': self.format_url(product.get('zoomImageAlternate', '')),
                        'filterType': self.safe_strip(product.get('filterType', '')),
                        'nonTransactionalWebSite': self.safe_strip(product.get('nonTransactionalWebSite', '')),
                        'isDiyProduct': str(product.get('isDiyProduct', False)).lower(),
                        'inStockEntry': str(product.get('inStockEntry', False)).lower(),
                        'inStoreStockEntry': str(product.get('inStoreStockEntry', False)).lower(),
                        'inStoreStockRegionalEntry': str(product.get('inStoreStockRegionalEntry', False)).lower(),
                        'visibleWithoutStock': str(product.get('visibleWithoutStock', False)).lower(),
                        'showAvailableInStoreOnlyLabel': str(product.get('showAvailableInStoreOnlyLabel', False)).lower(),
                        'showOutOfStockLabel': str(product.get('showOutOfStockLabel', False)).lower(),
                    }
                    self.logger.info(f"Parsed product info: {product_info}")
                    all_products.append(product_info)
                self.logger.info(f"Processed {len(items)} products on Page: {page + 1}/{total_pages} for Category: {category} URL: {current_url}")
            self.logger.info(f"All product info for category {category}: {all_products}")
            return pd.DataFrame(all_products)
        except Exception as e:
            self.logger.error(f"Error fetching data for category {category}: {traceback.format_exc()}")
            return pd.DataFrame()

    def process_categories(self, categories, locales):
        for locale in locales:
            self.data = pd.DataFrame()
            for category in categories:
                category_data = self.fetch_data(category, self.base_url, locale)
                self.data = pd.concat([self.data, category_data], ignore_index=True)
        current_date = datetime.datetime.now().strftime("%m_%d_%Y")
        self.output_filename = f"{self.brand}_output_{current_date}_{self.code}.csv"
        self.data.to_csv(self.output_filename, sep=',', index=False, quoting=csv.QUOTE_ALL)
        self.upload_url = self.upload_file_to_space(self.output_filename, self.output_filename)
        self.logger.info(f"Complete data saved to {self.output_filename}")
        self.count = len(self.data) - 1
        self.send_output()

class LoroPianaProductParser(WebsiteParser):
    def __init__(self, job_id, base_url):
        super().__init__(job_id, 'loro_piana')
        self.base_url = base_url
        self.data = pd.DataFrame()

    def fetch_data(self, category, base_url, country_code, locale):
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["HEAD", "GET", "OPTIONS"])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'}
        all_products = []

        try:
            current_url = base_url.format(category=category, country_code=country_code, locale=locale, page=0)
            self.logger.info(f"Fetching URL: {current_url}")
            response = session.get(current_url, headers=headers)
            response.raise_for_status()
            json_data = response.json()
            total_pages = json_data.get('pagination', {}).get("numberOfPages", 0)
            self.logger.info(f"Category: {category}, Total Pages: {total_pages}")

            for page in range(total_pages):
                current_url = base_url.format(category=category, country_code=country_code, locale=locale, page=page)
                response = session.get(current_url, headers=headers)
                response.raise_for_status()
                json_data = response.json()
                items = json_data.get('results', {})
                if not items:
                    self.logger.info(f"No items found on Page: {page+1}/{total_pages}")
                    continue

                for data in items:
                    self.logger.debug(f"Parsing product JSON: {data}")
                    product_info = {
                        'code': self.safe_strip(data.get('code', '')),
                        'solrIsFeatured': str(data.get('solrIsFeatured', False)).lower(),
                        'invertedImages': str(data.get('invertedImages', False)).lower(),
                        'genderFluid': str(data.get('genderFluid', False)).lower(),
                        'isAvailable': str(data.get('isAvailable', False)).lower(),
                        'variantsNr': data.get('variantsNr', ''),
                        'price_currencyIso': self.safe_strip(data.get('price', {}).get('currencyIso', '')),
                        'price_value': data.get('price', {}).get('value', ''),
                        'price_priceType': self.safe_strip(data.get('price', {}).get('priceType', '')),
                        'price_formattedValue': self.safe_strip(data.get('price', {}).get('formattedValue', '')),
                        'price_minQuantity': data.get('price', {}).get('minQuantity', None),
                        'price_maxQuantity': data.get('price', {}).get('maxQuantity', None),
                        'primaryImage': data.get('images', [])[0].get('url', '') if data.get('images') else '',
                        'alternateGalleryImages': " | ".join([img.get('url', '') for img in data.get('images', [])[1:]]),
                        'configurable': str(data.get('configurable', False)).lower(),
                        'name': self.safe_strip(data.get('name', '')),
                        'eshopMaterialCode': self.safe_strip(data.get('eshopMaterialCode', '')),
                        'gtmInfo': self.safe_strip(data.get('gtmInfo', '')),
                        'alternativeUrl': self.safe_strip(data.get('alternativeUrl', '')),
                        'relativeUrl': self.safe_strip(data.get('relativeUrl', '')),
                        'url': self.safe_strip(data.get('url', '')),
                        'colors': self.safe_strip(data.get('colors', '')),
                        'variantSizes': self.safe_strip(data.get('variantSizes', '')),
                        'allColorVariants': data.get('allColorVariants', []),
                        'productsInLook': self.safe_strip(data.get('productsInLook', '')),
                        'configurableMto': str(data.get('configurableMto', False)).lower(),
                        'configurableScarves': str(data.get('configurableScarves', False)).lower(),
                        'doubleGender': self.safe_strip(data.get('doubleGender', '')),
                        'preorderable': str(data.get('preorderable', False)).lower(),
                        'backorderable': self.safe_strip(data.get('backorderable', '')),
                        'flPreviewProduct': str(data.get('flPreviewProduct', False)).lower(),
                        'digitalUrl': self.safe_strip(data.get('digitalUrl', '')),
                        'description': self.safe_strip(data.get('description', '')),
                        'eshopValid': str(data.get('eshopValid', False)).lower(),
                        'forceMrf': str(data.get('forceMrf', False)).lower(),
                        'normalProductEshopValid': str(data.get('normalProductEshopValid', False)).lower(),
                    }
                    self.logger.info(f"Parsed product info: {product_info}")
                    all_products.append(product_info)
                self.logger.info(f"Processed {len(items)} products on Page: {page+1}/{total_pages} for Category: {category} URL: {current_url}")
            self.logger.info(f"All product data for category {category}: {all_products}")
            return pd.DataFrame(all_products)
        except Exception as e:
            self.logger.error(f"Error fetching data for category {category}: {traceback.format_exc()}")
            return pd.DataFrame()

    def process_categories(self, categories, country_dicts):
        for country_dict in country_dicts:
            locale = country_dict['locale']
            country_code = country_dict['country_code']
            for category in categories:
                category_data = self.fetch_data(category, self.base_url, country_code, locale)
                self.data = pd.concat([self.data, category_data], ignore_index=True)
        current_date = datetime.datetime.now().strftime("%m_%d_%Y")
        self.output_filename = f"{self.brand}_output_{current_date}_{self.code}.csv"
        self.data.to_csv(self.output_filename, sep=',', index=False, quoting=csv.QUOTE_ALL)
        self.upload_url = self.upload_file_to_space(self.output_filename, self.output_filename)
        self.logger.info(f"Complete data saved to {self.output_filename}")
        self.count = len(self.data) - 1
        self.send_output()

class AlexanderMcqueenParser(WebsiteParser):
    def __init__(self, job_id, base_url):
        super().__init__(job_id, 'alexander_mcqueen')
        self.base_url = base_url
        self.data = pd.DataFrame()

    def fetch_data(self, category, base_url, locale):
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36'}
        all_products = []

        try:
            current_url = base_url.format(clothing_category=category, locale=locale, page=0)
            self.logger.info(f"Fetching URL: {current_url}")
            response = session.get(current_url, headers=headers)
            response.raise_for_status()
            json_data = response.json()
            total_pages = json_data['stats']['nbPages'] + 1
            self.logger.info(f"Category: {category}, Total Pages: {total_pages}")

            for page in range(total_pages):
                current_url = base_url.format(clothing_category=category, locale=locale, page=page)
                response = session.get(current_url, headers=headers)
                response.raise_for_status()
                json_data = response.json()
                products = json_data['products']
                if not products:
                    self.logger.info(f"No items found on Page: {page + 1}/{total_pages} URL: {current_url}")
                    continue

                for product in products:
                    self.logger.debug(f"Parsing product JSON: {product}")
                    images = product.get('images', [])
                    product_info = {
                        'category': category,
                        'id': product.get('id', ''),
                        'isSku': product.get('isSku', ''),
                        'isSmc': product.get('isSmc', ''),
                        'name': product.get('name', ''),
                        'microColor': product.get('microColor', ''),
                        'microColorHexa': product.get('microColorHexa', ''),
                        'color': product.get('color', ''),
                        'size': product.get('size', ''),
                        'styleMaterialColor': product.get('styleMaterialColor', ''),
                        'brightcoveId': product.get('brightcoveId', ''),
                        'images': " | ".join([self.format_url(img['src']) for img in images]),
                        'bornSeasonDesc': product.get('bornSeasonDesc', ''),
                        'macroCategory': product['categories'].get('macroCategory', ''),
                        'superMicroCategory_en_US': product['categories'].get('superMicroCategory', {}).get('en_US', ''),
                        'url': "https://www.alexandermcqueen.com" + product.get('url', ''),
                        'smcUrl': "https://www.alexandermcqueen.com" + product.get('smcUrl', ''),
                        'alternativeAsset': self.format_url(product.get('alternativeAsset', {}).get('src', '')),
                        'price_hasSalePrice': product.get('price', {}).get('hasSalePrice', ''),
                        'price_currencyCode': product.get('price', {}).get('currencyCode', ''),
                        'price_percentageOff': product.get('price', {}).get('percentageOff', ''),
                        'price_listPrice': product.get('price', {}).get('listPrice', ''),
                        'price_salePrice': product.get('price', {}).get('salePrice', ''),
                        'price_finalPrice': product.get('price', {}).get('finalPrice', '')
                    }
                    self.logger.info(f"Parsed product info: {product_info}")
                    all_products.append(product_info)
                self.logger.info(f"Processed {len(products)} products on Page: {page + 1}/{total_pages} for Category: {category} URL: {current_url}")
            self.logger.info(f"All product data for category {category}: {all_products}")
            return pd.DataFrame(all_products)
        except Exception as e:
            self.logger.error(f"Error fetching data for category {category}: {traceback.format_exc()}")
            return pd.DataFrame()

    def process_categories(self, category_dicts, locales):
        for locale in locales:
            self.data = pd.DataFrame()
            for category_dict in category_dicts:
                categories = category_dict["category_list"]
                self.base_url = category_dict["base_url"]
                for category in categories:
                    category_data = self.fetch_data(category, self.base_url, locale)
                    self.data = pd.concat([self.data, category_data], ignore_index=True)
        current_date = datetime.datetime.now().strftime("%m_%d_%Y")
        self.output_filename = f"{self.brand}_output_{current_date}_{self.code}.csv"
        self.data.to_csv(self.output_filename, sep=',', index=False, quoting=csv.QUOTE_ALL)
        self.upload_url = self.upload_file_to_space(self.output_filename, self.output_filename)
        self.logger.info(f"Complete data saved to {self.output_filename}")
        self.count = len(self.data) - 1
        self.send_output()

class MonclerProductParser(WebsiteParser):
    def __init__(self, job_id, base_url):
        super().__init__(job_id, 'moncler')
        self.base_url = base_url
        self.country = ''
        self.data = pd.DataFrame()
        options = Options()
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        options.add_argument("--start-maximized")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-setuid-sandbox')
        options.add_argument("--headless=new")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

    def fetch_moncler_products(self, categories, country_code):
        all_products = []
        for category in categories:
            offset = 0
            while True:
                formatted_url = self.base_url.format(category=category, country_code=country_code)
                self.logger.info(f"Fetching URL: {formatted_url}")
                self.driver.get(formatted_url)
                self.country = country_code.split('/')[-1]
                response = self.driver.find_element(By.TAG_NAME, "body").text
                self.logger.debug(f"Response type: {type(response)}, First 1000 chars: {response[:1000]}")
                data = json.loads(response)
                products = data['data']['products']
                if not products:
                    break

                for product in products:
                    self.logger.debug(f"Parsing product: {product}")
                    product_info = {
                        'id': product.get('id', ''),
                        'productName': product.get('productName', ''),
                        'shortDescription': product.get('shortDescription', ''),
                        'productUrl': "https://www.moncler.com" + product.get('productUrl', ''),
                        'price': product.get('price', {}).get('sales', {}).get('formatted', ''),
                        'price_min': product.get('price', {}).get('min', {}).get('sales', {}).get('formatted', ''),
                        'price_max': product.get('price', {}).get('max', {}).get('sales', {}).get('formatted', ''),
                        'imageUrls': [img for img in product.get('imgs', {}).get('urls', [])],
                        'productCharacteristics': product.get('productCharacteristics', ''),
                        'variationAttributes': self.parse_variation_attributes(product.get('variationAttributes', []))
                    }
                    self.logger.info(f"Parsed product info: {product_info}")
                    all_products.append(product_info)

                offset += len(products)
                self.logger.info(f'Found {len(products)} products on offset {offset}')
                total_count = data['data']['count']
                if offset >= total_count:
                    break
            self.logger.info(f"All product data for category {category}: {all_products}")
        return pd.DataFrame(all_products)

    def parse_variation_attributes(self, variation_attributes):
        attributes = {}
        for attr in variation_attributes:
            if 'values' in attr:
                values = ', '.join([f"{v['displayValue']} (ID: {v['id']})" for v in attr['values']])
            else:
                values = attr.get('displayValue', '')
            attributes[attr['displayName']] = values
        return attributes

    def process_categories(self, categories, country_codes):
        all_data = pd.DataFrame()
        for country_code in country_codes:
            some_data = self.fetch_moncler_products(categories, country_code)
            all_data = pd.concat([all_data, some_data], ignore_index=True)
            self.logger.info(f"Data head for country code {country_code}: {all_data.head()}")
        current_date = datetime.datetime.now().strftime("%m_%d_%Y")
        self.output_filename = f"{self.brand}_output_{current_date}_{self.code}.csv"
        all_data.to_csv(self.output_filename, sep=',', index=False, quoting=csv.QUOTE_ALL)
        self.upload_url = self.upload_file_to_space(self.output_filename, self.output_filename)
        self.logger.info(f"Complete data saved to {self.output_filename}")
        self.count = len(all_data) - 1
        self.send_output()

class SaintLaurentProductParser(WebsiteParser):
    def __init__(self, job_id, base_url):
        super().__init__(job_id, 'saint_laurent')
        self.base_url = base_url
        self.data = pd.DataFrame()

    def fetch_data(self, category, base_url, locale):
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["HEAD", "GET", "OPTIONS"])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.3'}
        all_products = []

        try:
            current_url = self.base_url.format(category=category, page=0, locale=locale)
            self.logger.info(f"Fetching URL: {current_url}")
            response = session.get(current_url, headers=headers)
            response.raise_for_status()
            json_data = response.json()
            total_pages = json_data.get('stats', {}).get('nbPages', 0)
            self.logger.info(f"Category: {category}, Total Pages: {total_pages}")

            for page in range(total_pages):
                current_url = self.base_url.format(category=category, page=page, locale Robotic Process Automation (RPA)locale)
                self.logger.info(f"Fetching URL: {current_url}")
                response = session.get(current_url, headers=headers)
                response.raise_for_status()
                json_data = response.json()
                items = json_data.get('products', [])
                hits = json_data.get('hitsAlgolia', [])
                if not (items and hits):
                    self.logger.info(f"No items found on page: {page}")
                    continue

                for product, hit in zip(items, hits):
                    self.logger.debug(f"Parsing product JSON: {product}, Hit: {hit}")
                    price_dict = product.get('price', 0)
                    product_info = {
                        'category': product.get('categories', {}).get('productCategory', ''),
                        'product_url': product.get('url', ''),
                        'product_color': product.get('color', ''),
                        'product_relatedColors': [{
                            'color': color.get('color', ''),
                            'colorHex': color.get('colorHex', ''),
                            'styleMaterialColor': color.get('styleMaterialColor', ''),
                            'swatchUrl': color.get('swatchUrl', ''),
                            'employeeSaleVisible': color.get('employeeSaleVisible', False)
                        } for color in product.get('relatedColors', [])],
                        'product_styleMaterialColor': product.get('styleMaterialColor', ''),
                        'product_thumbnailUrls': product.get('thumbnailUrls', []),
                        'product_inStock': product.get('inStock', False),
                        'product_stock': product.get('stock', 0),
                        'product_categoryIds': product.get('categoryIds', []),
                        'product_ID': product.get('id', ''),
                        'product_bornSeasonDesc': product.get('bornSeasonDesc', ''),
                        'product_name': product.get('name', ''),
                        'product_microColor': product.get('microColor', ''),
                        'product_image': product.get('image', {}).get('src', ''),
                        'product_images': " | ".join([image.get('srcset', '') for image in product.get('images', {})]),
                        'hit_id': hit.get('id', ''),
                        'hit_isSku': hit.get('isSku', False),
                        'hit_size': hit.get('size', ''),
                        'hit_categories': hit.get('categories', {}),
                        'hit_imageThumbnail': hit.get('imageThumbnail', {}).get('src', ''),
                        'hit_priceDetails': hit.get('price', {}),
                        'hit_formattedSize': hit.get('formattedSize', ''),
                        'hit_swatches': [{
                            'smcId': swatch.get('smcId', ''),
                            'microColorHexa': swatch.get('microColorHexa', ''),
                            'microColor': swatch.get('microColor', ''),
                            'swatchImage': swatch.get('swatchImage', ''),
                            'url': swatch.get('url', '')
                        } for swatch in product.get('swatches', [])],
                        'price_id': price_dict.get('id', ''),
                        'price_has_sale_price': price_dict.get('hasSalePrice', ''),
                        'price_currency': price_dict.get('currencyCode', ''),
                        'price_percentageOff': price_dict.get('percentageOff', ''),
                        'sale_price': price_dict.get('salePrice', ''),
                        'list_price': price_dict.get('listPrice', ''),
                        'final_price': price_dict.get('finalPrice', ''),
                        'has_empl_sale': price_dict.get('hasEmployeeSalePromotion', ''),
                        'isPriceOnDemand': price_dict.get('isPriceOnDemand', '')
                    }
                    self.logger.info(f"Parsed product info: {product_info}")
                    all_products.append(product_info)
                self.logger.info(f"Processed {len(items)} products on page: {page} for Category: {category}")
            self.logger.info(f"All product data for category {category}: {all_products}")
            return pd.DataFrame(all_products)
        except Exception as e:
            self.logger.error(f"Error fetching data for category {category}: {traceback.format_exc()}")
            return pd.DataFrame()

    def process_categories(self, categories, locales):
        for locale in locales:
            for category in categories:
                category_data = self.fetch_data(category, self.base_url, locale)
                self.data = pd.concat([self.data, category_data], ignore_index=True)
        current_date = datetime.datetime.now().strftime("%m_%d_%Y")
        self.output_filename = f"{self.brand}_output_{current_date}_{self.code}.csv"
        self.data.to_csv(self.output_filename, sep=',', index=False, quoting=csv.QUOTE_ALL)
        self.upload_url = self.upload_file_to_space(self.output_filename, self.output_filename)
        self.logger.info(f"Complete data saved to {self.output_filename}")
        self.count = len(self.data) - 1
        self.send_output()

class DolceGabbanaProductParser(WebsiteParser):
    def __init__(self, job_id, base_url):
        super().__init__(job_id, 'dolce_gabbana')
        self.base_url = base_url
        self.data = pd.DataFrame()
        options = Options()
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        options.add_argument("--start-maximized")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-setuid-sandbox')
        options.add_argument("--headless=new")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

    def get_bearer_token(self):
        self.driver.get("https://www.dolcegabbana.com/en-us/fashion/women/bags/handbags/")
        for _ in range(2):
            load_more_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button.Button__btn--_SznX.CategoryPaginationLoadMore__category-pagination__load-more--SOwaX.Button__btn--secondary--Tpjc0'))
            )
            self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
            self.driver.execute_script("arguments[0].click();", load_more_button)
            time.sleep(1)

        logs = self.driver.get_log("performance")
        for entry in logs:
            if "Bearer" in str(entry.get("message", {})):
                token = json.loads(entry.get("message", {})).get("message", {}).get("params", {}).get('request', {}).get("headers", {}).get("Authorization")
                if token and token.startswith("Bearer "):
                    token = token.split(" ")[-1]
                    self.logger.info(f"Bearer token found: {token}")
                    return token
        self.logger.error("Bearer token not found")
        return None

    def fetch_products(self, category, info_dict):
        bearer_token = self.get_bearer_token()
        if not bearer_token:
            self.logger.error("Cannot fetch products without bearer token")
            return pd.DataFrame()

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Authorization': f'Bearer {bearer_token}'
        }
        all_products = []
        offset = 0

        while True:
            locale = info_dict['locale']
            site_id = info_dict['site_id']
            limit = info_dict['limit']
            formatted_url = self.base_url.format(offset=offset, locale=locale, site_id=site_id, limit=limit, category=category)
            self.logger.info(f"Fetching URL: {formatted_url}")
            response = requests.get(formatted_url, headers=headers)
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")
                break
            data = response.json()
            products = data.get('hits', [])
            if not products:
                self.logger.info("No more products to fetch.")
                break

            for product in products:
                self.logger.debug(f"Parsing product JSON: {product}")
                images = product.get('image', {})
                images_formatted = f"{images.get('link', '')} ({images.get('alt', '')})" if images else "No image"
                product_info = {
                    'category': category,
                    'productId': product.get('productId', ''),
                    'productName': product.get('productName', ''),
                    'price': product.get('price', 0),
                    'pricePerUnit': product.get('pricePerUnit', 0),
                    'currency': product.get('currency', ''),
                    'hitType': product.get('hitType', ''),
                    'productType_variationGroup': product['productType'].get('variationGroup', False),
                    'orderable': product.get('orderable', False),
                    'representedProduct_id': product['representedProduct'].get('id', ''),
                    'representedProduct_ids': ' | '.join([rp['id'] for rp in product.get('representedProducts', [])]),
                    'images': images_formatted,
                    'c_url': "https://www.dolcegabbana.com" + product.get('c_url', '')
                }
                self.logger.info(f"Parsed product info: {product_info}")
                all_products.append(product_info)

            self.logger.info(f"Fetched {len(products)} products from offset {offset} and URL {formatted_url}")
            offset += limit
            if offset >= data['total']:
                break
        self.logger.info(f"All product data for category {category}: {all_products}")
        return pd.DataFrame(all_products)

    def process_categories(self, categories, info_dicts):
        all_data = pd.DataFrame()
        for info_dict in info_dicts:
            self.data = pd.DataFrame()
            if categories:
                for category in categories:
                    self.logger.info(f"Fetching products for category: {category}")
                    category_data = self.fetch_products(category, info_dict)
                    all_data = pd.concat([all_data, category_data], ignore_index=True)
                    self.logger.info(f"Completed fetching for category: {category}")
            else:
                self.logger.info(f"Fetching products for all of Dolce")
                category_data = self.fetch_products("", info_dict)
                all_data = pd.concat([all_data, category_data], ignore_index=True)
                self.logger.info(f"Completed fetching for all of Dolce")
        current_date = datetime.datetime.now().strftime("%m_%d_%Y")
        self.output_filename = f"{self.brand}_output_{current_date}_{self.code}.csv"
        self.data.to_csv(self.output_filename, sep=',', index=False, quoting=csv.QUOTE_ALL)
        self.upload_url = self.upload_file_to_space(self.output_filename, self.output_filename)
        self.logger.info(f"Complete data saved to {self.output_filename}")
        self.count = len(self.data) - 1
        self.send_output()

class LoeweProductParser(WebsiteParser):
    def __init__(self, job_id, base_url):
        super().__init__(job_id, 'loewe')
        self.base_url = base_url
        self.data = pd.DataFrame()
        options = Options()
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        options.add_argument("--start-maximized")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-setuid-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--headless=new")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

    def get_bearer_token(self):
        self.driver.get("https://www.loewe.com/usa/en/women/shoes")
        for _ in range(2):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self.driver.execute_script("window.scrollBy(0, -arguments[0]);", 100)
            time.sleep(1)

        logs = self.driver.get_log("performance")
        for entry in logs:
            if "Bearer" in str(entry.get("message", {})):
                token = json.loads(entry.get("message", {})).get("message", {}).get("params", {}).get('request', {}).get("headers", {}).get("Authorization")
                if token and token.startswith("Bearer "):
                    token = token.split(" ")[-1]
                    self.logger.info(f"Bearer token found: {token}")
                    return token
        self.logger.error("Bearer token not found")
        return None

    def fetch_data(self, category, base_url, country_dict):
        bearer_token = self.get_bearer_token()
        if not bearer_token:
            self.logger.error("Cannot fetch products without bearer token")
            return pd.DataFrame()

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["HEAD", "GET", "OPTIONS"])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'Authorization': f'Bearer {bearer_token}'
        }
        all_products = []
        limit = country_dict['limit']
        country_code = country_dict['country_code']
        locale = country_dict['locale']
        site_id = country_dict['site_id']

        try:
            offset = 0
            current_url = base_url.format(category=category, offset=offset, limit=2, country_code=country_code, locale=locale, site_id=site_id)
            self.logger.info(f"Fetching URL: {current_url}")
            response = session.get(current_url, headers=headers)
            response.raise_for_status()
            json_data = response.json()
            items = json_data.get('hits', [])
            total_products = int(items[0].get('c_totalProducts', '').replace(',', '').replace('.', '')) if items else 0

            while offset <= total_products:
                current_url = base_url.format(category=category, offset=offset, limit=limit, country_code=country_code, locale=locale, site_id=site_id)
                self.logger.info(f"Fetching URL: {current_url}")
                response = session.get(current_url, headers=headers)
                response.raise_for_status()
                json_data = response.json()
                items = json_data.get('hits', [])
                if not items:
                    self.logger.info(f"No items found on offset: {offset}")
                    continue

                for product in items:
                    self.logger.debug(f"Parsing product JSON: {product}")
                    product_data = product.get('c_gtm_data', {})
                    all_images = json.loads(product.get('c_allImages', '[]'))
                    color_swatches = json.loads(product.get('c_colorSwatches', '[]'))
                    product_info = {
                        'brand': product_data.get('brand', ''),
                        'category': product_data.get('category', ''),
                        'id': product_data.get('id', ''),
                        'name': product_data.get('name', ''),
                        'price_gtm': product_data.get('price', ''),
                        'productColor': product_data.get('productColor', ''),
                        'colorId': product_data.get('colorId', ''),
                        'productEan': product_data.get('productEan', ''),
                        'productGender': product_data.get('productGender', ''),
                        'productMasterId': product_data.get('productMasterId', ''),
                        'productStock': product_data.get('productStock', ''),
                        'isDiscounted': product_data.get('isDiscounted', ''),
                        'position_gtm': product_data.get('position', ''),
                        'currency': product.get('currency', ''),
                        'image': {
                            'alt': product.get('image', {}).get('alt', ''),
                            'disBaseLink': product.get('image', {}).get('disBaseLink', ''),
                            'link': product.get('image', {}).get('link', ''),
                            'title': product.get('image', {}).get('title', '')
                        },
                        'imageUrl': product.get('image', {}).get('link', ''),
                        'orderable': product.get('orderable', False),
                        'price': product.get('price', ''),
                        'pricePerUnit': product.get('pricePerUnit', ''),
                        'productId': product.get('productId', ''),
                        'productName': product.get('productName', ''),
                        'productType': product.get('productType', {}),
                        'representedProduct': product.get('representedProduct', {}),
                        'representedProducts': product.get('representedProducts', []),
                        'c_totalProducts': product.get('c_totalProducts', ''),
                        'c_lineImagePath': product.get('c_lineImagePath', ''),
                        'c_productDetailPageURL': product.get('c_productDetailPageURL', ''),
                        'c_imageURL': product.get('c_imageURL', ''),
                        'c_isPromoPrice': product.get('c_isPromoPrice', False),
                        'c_showStandardPrice': product.get('c_showStandardPrice', False),
                        'c_salesPriceFormatted': product.get('c_salesPriceFormatted', ''),
                        'c_standardPriceFormatted': product.get('c_standardPriceFormatted', ''),
                        'c_hidePrice': product.get('c_hidePrice', False),
                        'c_colorSwatches': color_swatches,
                        'c_colorSelected': product.get('c_colorSelected', ''),
                        'c_allImages': all_images,
                        'c_LW_limiterColorBadges': product.get('c_LW_limiterColorBadges', ''),
                        'c_availabilityStatus': product.get('c_availabilityStatus', ''),
                        'c_productDetailUrlComplete': product.get('c_productDetailUrlComplete', '')
                    }
                    self.logger.info(f"Parsed product info: {product_info}")
                    all_products.append(product_info)
                self.logger.info(f"Processed {len(items)} products on offset: {offset} for Category: {category} URL: {current_url}")
                offset += limit
                self.logger.info(f"Offset: {offset}")
            self.logger.info(f"All product data for category {category}: {all_products}")
            return pd.DataFrame(all_products)
        except Exception as e:
            self.logger.error(f"Error fetching data for category {category}: {traceback.format_exc()}")
            return pd.DataFrame()

    def process_categories(self, categories, country_dicts):
        for country_dict in country_dicts:
            self.data = pd.DataFrame()
            for category in categories:
                category_data = self.fetch_data(category, self.base_url, country_dict)
                self.data = pd.concat([self.data, category_data], ignore_index=True)
        current_date = datetime.datetime.now().strftime("%m_%d_%Y")
        self.output_filename = f"{self.brand}_output_{current_date}_{self.code}.csv"
        self.data.to_csv(self.output_filename, sep=',', index=False, quoting=csv.QUOTE_ALL)
        self.upload_url = self.upload_file_to_space(self.output_filename, self.output_filename)
        self.logger.info(f"Complete data saved to {self.output_filename}")
        self.count = len(self.data) - 1
        self.send_output()

class StoneIslandProductParser(WebsiteParser):
    def __init__(self, job_id, base_url):
        super().__init__(job_id, 'stone_island')
        self.base_url = base_url
        self.data = pd.DataFrame()

    def fetch_data(self, category, locale_dict):
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["HEAD", "GET", "OPTIONS"])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'}
        all_products = []
        locale = locale_dict.get('locale', '')
        size = locale_dict.get('size', '')
        start = locale_dict.get('start', '')

        try:
            current_url = self.base_url.format(category=category, size=size, start=start, locale=locale)
            self.logger.info(f"Fetching URL: {current_url}")
            response = session.get(current_url, headers=headers)
            response.raise_for_status()
            json_data = response.json()
            if isinstance(json_data, str):
                json_data = json.loads(json_data)
            num_products = json_data.get('data', {}).get("count", "")

            while start <= num_products:
                current_url = self.base_url.format(category=category, size=size, start=start, locale=locale)
                self.logger.info(f"Fetching URL: {current_url}")
                response = session.get(current_url, headers=headers)
                response.raise_for_status()
                json_data = response.json()
                if isinstance(json_data, str):
                    json_data = json.loads(json_data)
                products = json_data.get('data', {}).get('products', [])
                start += size

                for product in products:
                    self.logger.debug(f"Parsing product JSON: {product}")
                    product_info = {
                        'category': category,
                        'type': self.safe_strip(product.get('type', '')),
                        'masterId': self.safe_strip(product.get('masterId', '')),
                        'uuid': self.safe_strip(product.get('uuid', '')),
                        'id': self.safe_strip(product.get('id', '')),
                        'productName': self.safe_strip(product.get('productName', '')),
                        'shortDescription': self.safe_strip(product.get('shortDescription', '')),
                        'productUrl': self.safe_strip(product.get('productUrl', '')),
                        'route': self.safe_strip(product.get('route', '')),
                        'originalModelName': self.safe_strip(product.get('originalModelName', '')),
                        'isComingSoon': str(product.get('isComingSoon', False)).lower(),
                        'price_sales_value': self.safe_strip(product.get('price', {}).get('sales', {}).get('value', '')),
                        'price_sales_currency': self.safe_strip(product.get('price', {}).get('sales', {}).get('currency', '')),
                        'price_sales_formatted': self.safe_strip(product.get('price', {}).get('sales', {}).get('formatted', '')),
                        'image_urls': ','.join(product.get('imgs', {}).get('urls', [])),
                        'image_alt': self.safe_strip(product.get('imgs', {}).get('alt', '')),
                        'analytics_item_name': self.safe_strip(product.get('analyticsAttributes', {}).get('item_name', '')),
                        'analytics_item_category': self.safe_strip(product.get('analyticsAttributes', {}).get('item_category', '')),
                        'analytics_item_category2': self.safe_strip(product.get('analyticsAttributes', {}).get('item_category2', '')),
                        'analytics_item_category3': self.safe_strip(product.get('analyticsAttributes', {}).get('item_category3', '')),
                        'analytics_item_category4': self.safe_strip(product.get('analyticsAttributes', {}).get('item_category4', '')),
                        'analytics_item_category5': self.safe_strip(product.get('analyticsAttributes', {}).get('item_category5', '')),
                        'analytics_item_variant': self.safe_strip(product.get('analyticsAttributes', {}).get('item_variant', '')),
                        'analytics_item_MFC': self.safe_strip(product.get('analyticsAttributes', {}).get('item_MFC', '')),
                        'availability_lowStock': str(product.get('availability', {}).get('lowStock', False)).lower(),
                        'available': str(product.get('available', False)).lower(),
                        'earlyaccess_private': str(product.get('earlyaccess', {}).get('private', False)).lower(),
                        'imageBackground': self.safe_strip(product.get('imageBackground', '')),
                        'assetOverride_plp': self.safe_strip(product.get('assetOverride', {}).get('plp', '')),
                        'assetOverride_plpeditorial': self.safe_strip(product.get('assetOverride', {}).get('plpeditorial', '')),
                        'assetOverride_icongallery': self.safe_strip(product.get('assetOverride', {}).get('icongallery', '')),
                        'seoName': self.safe_strip(product.get('seoName', '')),
                    }

                    for attr in product.get('variationAttributes', []):
                        attr_id = attr.get('attributeId', '')
                        product_info[f'variationAttribute_{attr_id}_displayName'] = self.safe_strip(attr.get('displayName', ''))
                        product_info[f'variationAttribute_{attr_id}_displayValue'] = self.safe_strip(attr.get('displayValue', ''))
                        product_info[f'variationAttribute_{attr_id}_swatchable'] = str(attr.get('swatchable', False)).lower()
                        product_info[f'variationAttribute_{attr_id}_values'] = ','.join(
                            [self.safe_strip(value.get('displayValue', '')) for value in attr.get('values', [])])

                    product_info.update(locale_dict)
                    self.logger.info(f"Parsed product info: {product_info}")
                    all_products.append(product_info)
                self.logger.info(f"All product data for category {category}: {all_products}")
            return pd.DataFrame(all_products)
        except Exception as e:
            self.logger.error(f"Error fetching data for category {category}: {traceback.format_exc()}")
            return pd.DataFrame()

    def process_categories(self, categories, locale_dicts):
        for locale_dict in locale_dicts:
            self.data = pd.DataFrame()
            for category in categories:
                category_data = self.fetch_data(category, locale_dict)
                self.data = pd.concat([self.data, category_data], ignore_index=True)
        current_date = datetime.datetime.now().strftime("%m_%d_%Y")
        self.output_filename = f"{self.brand}_output_{current_date}_{self.code}.csv"
        self.data.to_csv(self.output_filename, sep=',', index=False, quoting=csv.QUOTE_ALL)
        self.upload_url = self.upload_file_to_space(self.output_filename, self.output_filename)
        self.logger.info(f"Complete data saved to {self.output_filename}")
        self.count = len(self.data) - 1
        self.send_output()

class ChloeProductParser(WebsiteParser):
    def __init__(self, job_id, base_url):
        super().__init__(job_id, 'chloe')
        self.base_url = base_url
        self.data = pd.DataFrame()
        options = Options()
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        options.add_argument("--start-maximized")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-setuid-sandbox')
        options.add_argument("--headless=new")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

    def fetch_data(self, category, locale_dict):
        locale = locale_dict.get("locale", "")
        size = locale_dict.get("size", "")
        self.logger.info(f"Processing size: {size}, locale: {locale}")
        try:
            locale_TF = True if "US" in locale else False
            current_url = self.base_url.format(category=category, size=size, locale=locale)
            self.logger.info(f"Fetching URL: {current_url}")
            self.driver.get(current_url)
            WebDriverWait(self.driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            product_html = self.driver.execute_script("return document.documentElement.outerHTML;")
            soup = BeautifulSoup(product_html, 'html.parser')
            product_info = self.get_product_info(soup, category, locale_TF)
            return pd.DataFrame(product_info)
        except Exception as e:
            self.logger.error(f"Error fetching data for category {category}: {traceback.format_exc()}")
            return pd.DataFrame()

    def extract_product_id(self, product_url, locale_TF):
        self.logger.info(f"Fetching product ID for URL: {product_url}")
        self.driver.get(product_url)
        try:
            WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except:
            self.logger.error(f"Could not find body tag for URL: {product_url}")
            return ""

        html = self.driver.page_source
        pid_text = 'Item code: ' if locale_TF else 'Codice articolo: '
        self.logger.info(f"Looking for PID text: {pid_text}")
        soup_pid = BeautifulSoup(html, 'html.parser') if html else None
        if soup_pid:
            main_item = soup_pid.find('div', class_='itemdescription')
            if main_item:
                style_id_text = main_item.text
                self.logger.debug(f"Product ID text on page: {style_id_text}")
                style_id = style_id_text.split(pid_text)[-1].strip()
                self.logger.info(f"Product ID: {style_id}")
                return style_id
            else:
                self.logger.info(f"Product ID not found for URL: {product_url}")
                return ""
        return ""

    def get_product_info(self, soup, category, locale_TF):
        self.logger.info(f"Extracting product data for category: {category}")
        parsed_data = []
        articles = soup.find_all('article', {'class': 'item'})

        for article in articles:
            product_data = {}
            img_source = article.find('img')
            img_source = img_source['src'] if img_source else ''
            data_pinfo = article['data-ytos-track-product-data']
            a_url = article.find('a')
            a_url = a_url['href'] if a_url else ''
            product_id = self.extract_product_id(a_url, locale_TF)
            product_info = json.loads(data_pinfo)

            product_data['Product_ID'] = product_id
            product_data['Cod10'] = product_info.get('product_cod10', '')
            product_data['Title'] = product_info.get('product_title', '')
            product_data['Price'] = product_info.get('product_price', '')
            product_data['position'] = product_info.get('product_position', '')
            product_data['category'] = product_info.get('product_category', '')
            product_data['macro_category'] = product_info.get('product_macro_category', '')
            product_data['micro_category'] = product_info.get('product_micro_category', '')
            product_data['macro_category_id'] = product_info.get('product_macro_category_id', '')
            product_data['micro_category_id'] = product_info.get('product_micro_category_id', '')
            product_data['color'] = product_info.get('product_color', '')
            product_data['color_id'] = product_info.get('product_color_id', '')
            product_data['product_price'] = product_info.get('product_price', '')
            product_data['discountedPrice'] = product_info.get('product_discountedPrice', '')
            product_data['price_tf'] = product_info.get('product_price_tf', '')
            product_data['discountedPrice_tf'] = product_info.get('product_discountedPrice_tf', '')
            product_data['quantity'] = product_info.get('product_quantity', '')
            product_data['coupon'] = product_info.get('product_coupon', '')
            product_data['is_in_stock'] = product_info.get('product_is_in_stock', '')
            product_data['list'] = product_info.get('list', '')
            product_data['url'] = a_url
            product_data['img_src'] = img_source
            product_data['category'] = category
            self.logger.info(f"Parsed product data: {product_data}")
            parsed_data.append(product_data)
        return parsed_data

    def process_categories(self, categories, locale_dicts):
        for locale_dict in locale_dicts:
            self.logger.info(f"Processing locale: {locale_dict}")
            self.data = pd.DataFrame()
            for category in categories:
                self.logger.info(f"Processing category: {category}")
                category_data = self.fetch_data(category, locale_dict)
                self.data = pd.concat([self.data, category_data], ignore_index=True)
        current_date = datetime.datetime.now().strftime("%m_%d_%Y")
        self.output_filename = f"{self.brand}_output_{current_date}_{self.code}.csv"
        self.data.to_csv(self.output_filename, sep=',', index=False, quoting=csv.QUOTE_ALL)
        self.upload_url = self.upload_file_to_space(self.output_filename, self.output_filename)
        self.logger.info(f"Complete data saved to {self.output_filename}")
        self.count = len(self.data) - 1
        self.send_output()

class CanadaGooseProductParser(WebsiteParser):
    def __init__(self, job_id, base_url):
        super().__init__(job_id, 'canada_goose')
        self.base_url = base_url
        self.data = pd.DataFrame()
        options = Options()
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
        options.add_argument("--headless=new")
        options.add_argument("--start-maximized")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-setuid-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

    def fetch_data(self, category, base_url, locale):
        all_products = []
        try:
            current_url = base_url.format(category=category, locale=locale)
            self.logger.info(f"Fetching URL: {current_url}")
            self.driver.get(current_url)
            WebDriverWait(self.driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            product_html = self.driver.execute_script("return document.documentElement.outerHTML;")
            soup = BeautifulSoup(product_html, 'html.parser')
            product_blocks = soup.find_all('div', class_='product')

            for block in product_blocks:
                self.logger.debug(f"Parsing product block")
                product_id = block.get('data-pid', 'No ID')
                product_link = block.find('a', class_='thumb-link')
                product_url = f"https://www.canadagoose.com{product_link['href']}" if product_link else 'No URL'
                product_name = block.find('div', class_='product-name').text.strip() if block.find('div', class_='product-name') else 'No Name'
                price_element = block.find('span', class_='price-sales')
                price = price_element.find('span', class_='value').text.strip() if price_element else 'No Price'

                image_urls = []
                images = block.find_all('div', class_='slideritem')
                for image in images:
                    img = image.find("img")
                    if img and 'srcset' in img.attrs:
                        srcset = img['srcset'].split(', ')
                        for src in srcset:
                            image_urls.append(src.split(' ')[0])
                    elif img and 'src' in img.attrs:
                        image_urls.append(img['src'])
                    elif img and 'data-src' in img.attrs:
                        image_urls.append(img['data-src'])

                color_options = []
                color_links = block.find_all('a', class_='swatch')
                for link in color_links:
                    color = link.get('title', 'No Color')
                    color_image = link.find('img').get('data-src') if link.find('img') else 'No Color Image'
                    color_options.append({'color': color, 'image': color_image})

                product_info = {
                    'product_id': product_id,
                    'product_url': product_url,
                    'product_name': product_name,
                    'price': price,
                    'image_urls': ', '.join(image_urls),
                    'color_options': json.dumps(color_options)
                }
                self.logger.info(f"Parsed product info: {product_info}")
                all_products.append(product_info)
            self.logger.info(f"All product data for category {category}: {all_products}")
            return pd.DataFrame(all_products)
        except Exception as e:
            self.logger.error(f"Error fetching data for category {category}: {traceback.format_exc()}")
            return pd.DataFrame()

    def process_categories(self, categories, locales):
        for locale in locales:
            self.data = pd.DataFrame()
            for category in categories:
                category_data = self.fetch_data(category, self.base_url, locale)
                self.data = pd.concat([self.data, category_data], ignore_index=True)
        current_date = datetime.datetime.now().strftime("%m_%d_%Y")
        self.output_filename = f"{self.brand}_output_{current_date}_{self.code}.csv"
        self.data.to_csv(self.output_filename, sep=',', index=False, quoting=csv.QUOTE_ALL)
        self.upload_url = self.upload_file_to_space(self.output_filename, self.output_filename)
        self.logger.info(f"Complete data saved to {self.output_filename}")
        self.count = len(self.data) - 1
        self.send_output()

if __name__ == "__main__":
    uvicorn.run("main:app", port=8080, host="0.0.0.0", log_level="info")