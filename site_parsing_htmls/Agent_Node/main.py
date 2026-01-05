import re
from urllib.parse import urljoin, urlunparse, urlparse
from selenium import webdriver
import traceback
import json
import html
import time
import logging
import logging.handlers
import uuid
import uvicorn
import boto3
from bs4 import BeautifulSoup
import csv
import datetime
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fastapi import FastAPI, BackgroundTasks

app = FastAPI(title="agent_brand_html_parser", version="2.0.1")
class WebsiteParser:
    def __init__(self, job_id, brand):
        self.output_filename = None
        self.upload_url = None
        self.count = 0
        self.log_url = None
        self.session = requests.Session()
        self.code = str(uuid.uuid4())
        self.job_id = job_id  # Set job_id
        self.brand = brand    # Set brand
        self.setup_logging()   # Call setup_logging after job_id and brand are set
        self.config = self.fetch_config()

    def fetch_config(self):
        try:
            config_url = "https://iconluxury.shop/secrets/secrets_scraper_config.json"
            response = requests.get(config_url, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            self.logger.error(f"Error fetching config: {e}")
        return {}

    def setup_logging(self):
        current_date = datetime.datetime.now().strftime("%d_%m_%Y")
        self.log_file_name = f'{self.brand}_{self.code}_{current_date}.log'
        logger_name = f"Brand ID: {self.brand}, Job ID: {self.job_id}, UUID: {self.code}"
        self.logger = logging.getLogger(logger_name)

        # Set formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Create handler for logs that go to the file on the debug level
        log_file_handler = logging.handlers.RotatingFileHandler(self.log_file_name)
        log_file_handler.setFormatter(formatter)
        log_file_handler.setLevel(logging.DEBUG)

        # Create handler for logs that go to the console on the info level
        log_console_handler = logging.StreamHandler()
        log_console_handler.setFormatter(formatter)
        log_console_handler.setLevel(logging.INFO)

        self.logger.addHandler(log_file_handler)
        self.logger.addHandler(log_console_handler)

        # Initial test that the logger is instantiated and working properly
        self.logger.info("This is what info messages will look like")
        self.logger.error("This is what error messages will look like")
        self.logger.critical("This is what a critical error message looks like")
        # Remove: self.logger = logging.getLogger(__name__)  # Avoid overriding the logger
    def convert_to_tsv(self, data):
        output = []
        for row in data:
            output.append([str(item) for item in row])

        return output

    def write_to_csv(self, csv_data):
        current_date = datetime.datetime.now().strftime("%d_%m_%Y")

        file_path = f'output_{self.brand}_{self.code}_{current_date}.csv'

        # Write data to CSV
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
        
        r2_settings = self.config.get("r2_settings", {})
        space_name = r2_settings.get("bucket_name", 'archive.iconluxurygroup')
        public_domain = r2_settings.get("public_domain")

        try:
            spaces_client.upload_file(
                str(file_src),
                space_name,
                str(save_as),
                ExtraArgs={'ACL': 'public-read'} if is_public else None
            )
            self.logger.info(f"File uploaded successfully to {space_name}/{save_as}")
            if is_public:
                if public_domain:
                    upload_url = f"{public_domain}/{save_as}"
                else:
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
                print(f"Public URL: {upload_url}")
                return upload_url
        except Exception:
            self.logger.error(f"Error occurred while uploading file to space: {traceback.format_exc()}")
            return None

    def get_s3_client(self):
        self.logger.info("Creating S3 client")
        try:
            region = os.getenv('AWS_REGION', 'us-east-2')
            session = boto3.session.Session()
            client = session.client(service_name='s3', region_name=region)
            self.logger.info("S3 client created successfully")
            return client
        except Exception:
            self.logger.error(f"Error occurred while creating S3 client: {traceback.format_exc()}")
            return None
    def parse_website(self, source):
        category = source
        html_content = self.open_link(source)
        if html_content is None:
            self.logger.error(f"Failed to fetch HTML content from {source}")
            return
        soup = BeautifulSoup(html_content, 'html.parser')
        parsed_data = self.parse_product_blocks(soup, category)
        all_data = parsed_data[0]
        all_data.append('source')
        all_data = [all_data]
        print(all_data)
        for row in parsed_data[1:]:
            row.extend([source])
            all_data.append(row)
        print(all_data)
        all_data = self.convert_to_tsv(all_data)
        file_name = self.write_to_csv(all_data)
        self.output_filename = file_name
        self.upload_url = self.upload_file_to_space(file_name, file_name)
        self.count = len(all_data) - 1
        self.log_url = self.upload_file_to_space(self.log_file_name, self.log_file_name)
        self.send_output()
    def send_output(self):
        logging.shutdown()
        headers = {
            'accept': 'application/json',
            'content-type': 'application/x-www-form-urlencoded',
        }

        params = {
            'job_id': f"{self.job_id}",
            'resultUrl': f"{self.upload_url}",
            'logUrl': f"{self.log_url}",
            'count': self.count
        }
                # Remove files and directory if they exist
        try:
            if os.path.exists(self.output_filename):
                os.remove(self.output_filename)
            if os.path.exists(self.log_file_name):
                os.remove(self.log_file_name)
        except Exception as e:
            self.logger.error(f"Error cleaning up files/directories: {traceback.format_exc()}")

        requests.post(f"{send_out_endpoint}/job_complete", params=params, headers=headers)
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
            print(url)
            response = session.get(url, headers=headers, allow_redirects=True, verify=False)  # Disable SSL verification
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None
class BottegaVenetaParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'bottega_veneta')

    #COMPLETE

    ## This class parses the HTML files from the Bottega Veneta website.
    ## website: https://www.bottegaveneta.com

    def parse_product_blocks(self, soup, category):
        product_blocks = soup.find_all('article', class_='c-product')
        parsed_data = []

        column_names = [
            'data_pid', 'id', 'name', 'collection', 'productSMC', 'material', 'customization',
            'packshotType', 'brand', 'color', 'colorId', 'size', 'price', 'discountPrice',
            'coupon', 'subCategory', 'category', 'topCategory', 'productCategory', 'macroCategory',
            'microCategory', 'superMicroCategory', 'list', 'stock', 'productGlobalSMC', 'images', 'product_url'
        ]

        parsed_data.append(column_names)

        for block in product_blocks:
            product_data = []

            # Extract data-pid
            data_pid = block['data-pid']

            # Extract data-gtmproduct and parse JSON
            data_gtmproduct = block['data-gtmproduct']
            data_gtmproduct = html.unescape(data_gtmproduct)
            product_info = json.loads(data_gtmproduct)
            # Extract product images
            images = []
            image_container = block.find('div', class_='c-product__imagecontainer')
            if image_container:
                image_elements = image_container.find_all('img', class_='c-product__image')
                for img in image_elements:
                    images.append(img.get('src', ''))
                    images.append(img.get('data-src', ''))
                    images.append(img.get('srcset', ''))
            images = list(set(images))
            images = [image for image in images if image != '' and "/on/demandware.static/" not in image]
            # Extract product URL
            product_url = block.find('a', class_='c-product__link')['href']

            # Append the extracted information to the product_data list
            product_data.append(data_pid)
            product_data.append(product_info['id'])
            product_data.append(product_info['name'])
            product_data.append(product_info['collection'])
            product_data.append(product_info['productSMC'])
            product_data.append(product_info['material'])
            product_data.append(product_info['customization'])
            product_data.append(product_info['packshotType'])
            product_data.append(product_info['brand'])
            product_data.append(product_info['color'])
            product_data.append(product_info['colorId'])
            product_data.append(product_info['size'])
            product_data.append(product_info['price'])
            product_data.append(product_info['discountPrice'])
            product_data.append(product_info['coupon'])
            product_data.append(product_info['subCategory'])
            product_data.append(category)
            product_data.append(product_info['topCategory'])
            product_data.append(product_info['productCategory'])
            product_data.append(product_info['macroCategory'])
            product_data.append(product_info['microCategory'])
            product_data.append(product_info['superMicroCategory'])
            product_data.append(product_info['list'])
            product_data.append(product_info['stock'])
            product_data.append(product_info['productGlobalSMC'])
            product_data.append(', '.join(images))
            product_data.append(product_url)

            parsed_data.append(product_data)

        return parsed_data
class FendiProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'fendi')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
          'product_id','category', 'product_name', 'description', 'price', 'main_image_url',
            'additional_image_url', 'product_details_url'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('div', class_='product')
        for block in product_blocks:
            try:
                product_id=block.get('data-pid')
                product_name = block.find('a', class_='link').text.strip() if block.find('a', class_='link') else ''
                description = block.find('p', class_='c-tiles__tile-body-type').text.strip() if block.find('p',
                                                                                                           'c-tiles__tile-body-type') else ''
                price = block.find('span', class_='value').text.strip() if block.find('span', class_='value') else ''
                images = block.find_all('img', class_='c-lazyload__image')

                main_image_url = images[0].get('src') or images[0].get('data-src') if images else None
                additional_image_url = images[1].get('src') or images[1].get('data-src') if len(images) > 1 else None
                product_details_url = block.find('a')['href'] if block.find('a') else ''

                product_data_list = [
                    product_id, category, product_name, description, price,
                    main_image_url, additional_image_url, product_details_url
                ]
                parsed_data.append(product_data_list)
            except Exception as e:
                print(f"Error parsing block: {e}")

        return parsed_data
class GivenchyProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'givenchy')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'user_category','product_id', 'product_url', 'product_name', 'price', 'image_urls','availability','currency','label'
        ]
        parsed_data.append(column_names)

        product_tiles = soup.find_all('div', class_='product-tile')

        for tile in product_tiles:
            product_id = tile.get('data-itemid', '')
            product_url = tile.find('a', class_='thumb-link')['href'] if tile.find('a', class_='thumb-link') else ''
            product_name = tile.find('h2', class_='product-name').text.strip() if tile.find('h2', class_='product-name') else ''
            price_element = tile.find('span', class_='price-sales')
            price = price_element.text.strip() if price_element else ''

            # Extract all image URLs
            image_elements = tile.find_all('img', class_='thumb-img')
            image_urls = [img.get('data-srcset').split()[0] for img in image_elements if img.get('data-srcset')]

            availability=tile.get('data-availability','')
            currency_meta = tile.find('meta', itemprop='priceCurrency')
            currency=''
            if currency_meta:
                currency = currency_meta.get('content','')
            label_element = soup.find('span', class_='tile-label')
            label = label_element.text.strip() if label_element else ''

            product_data = [
                category,
                product_id,
                product_url,
                product_name,
                price,
                ', '.join(image_urls),
                availability,
                currency,
                label
            ]
            parsed_data.append(product_data)

        return parsed_data
class CanadaGooseProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'canada_goose')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'product_id', 'product_url', 'product_name', 'price', 'image_urls', 'color_options'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('div', class_='product')

        for block in product_blocks:
            # Extract product ID from the data-pid attribute
            product_id = block.get('data-pid', 'No ID')

            # Extract product URL
            product_link = block.find('a', class_='thumb-link')
            product_url = product_link['href'] if product_link else 'No URL'

            # Extract product name
            product_name = block.find('div', class_='product-name').text.strip() if block.find('div', class_='product-name') else 'No Name'

            # Extract price
            price_element = block.find('span', class_='price-sales')
            price = price_element.find('span', class_='value').text.strip() if price_element else 'No Price'

            # Extract image URLs
            image_urls = []
            images = block.find_all('div', class_='slideritem')
            for image in images:
                img=image.find("img")
                if 'srcset' in img.attrs:
                    srcset = img['srcset'].split(', ')
                    for src in srcset:
                        image_urls.append(src.split(' ')[0])
                elif 'src' in img.attrs:
                    image_urls.append(img['src'])
                elif 'data-src' in img.attrs:
                    image_urls.append(img['data-src'])

            # Extract color options
            color_options = []
            color_links = block.find_all('a', class_='swatch')
            for link in color_links:
                color = link.get('title', 'No Color')
                color_image = link.find('img').get('data-src') if link.find('img') else 'No Color Image'
                color_options.append({'color': color, 'image': color_image})

            product_data = [
                product_id,
                f"https://www.canadagoose.com{product_url}",
                product_name,
                price,
                ', '.join(image_urls),
                json.dumps(color_options)  # Store color options as JSON string for simplicity
            ]
            parsed_data.append(product_data)

        return parsed_data
class VejaProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'veja')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'category', 'product_id', 'product_name', 'price', 'discount_price', 'main_image_url',
            'additional_image_url',
            'product_details_url'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('div', class_='product-item-info')

        for block in product_blocks:
            product_name = block.find('strong', class_='product name product-item-name').find('a').text.strip()
            full_price_element = block.find('span', class_='old-price')
            if full_price_element:
                discount_price_element = block.find('span', class_='normal-price')
                price = full_price_element.find('span', class_='price').text.strip()
                discount_price = discount_price_element.find('span', class_='price').text.strip()
            else:
                price_element = block.find('span', class_='normal-price')
                price = price_element.find('span', class_='price').text.strip()
                discount_price = ''
            product_details_url = block.find('a', class_='product-item-link')['href']

            images = block.find_all('img', alt=product_name)
            main_image_url = images[0]['src'] if images else 'No image available'
            additional_image_url = images[1]['src'] if len(images) > 1 else 'No additional image available'
            product_id = product_details_url.split("-")[-1].replace(".html", '')
            product_data_list = [
                category,
                product_id,
                product_name,
                price,
                discount_price,
                main_image_url,
                additional_image_url,
                product_details_url
            ]

            parsed_data.append(product_data_list)

        return parsed_data
class StellaProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'stella_mccartney')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'product_id', 'category', 'product_name', 'price', 'main_image_url', 'additional_image_url',
            'product_details_url', 'product_color', 'product_tags'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('div', class_='product')

        for block in product_blocks:
            product_id = block.get('data-pid', '')
            product_name = block.get('data-name', '')
            price_element = block.find('span', class_='value')
            price = price_element.text.strip() if price_element else ''
            link_element = block.find('a', class_='lazy__link')
            product_details_url = link_element['href'] if link_element else ''
            # product-tile__plp-images-stack
            # Handling images
            images_data = block.find('div', class_='product-tile__plp-images-stack')
            product_images_list = images_data.get('data-product-images',[]).strip()
            product_images_list = json.loads(product_images_list)



            try:
                main_image_url = product_images_list[0].get('url','') if product_images_list else ''
                additional_image_url = " , ".join([s.get('url') for s in product_images_list ])

            except json.JSONDecodeError:
                main_image_url = ''
                additional_image_url = ''

            # Handling color options from buttons
            color_buttons = block.find_all('button', class_='more')
            colors = [btn.get('aria-label').split(' ')[-1] for btn in color_buttons if btn.get('aria-label')]

            # Handling tags
            tags_div = block.find('div', class_='product-tile__body__tags')
            tags = [tag.text for tag in tags_div.find_all('div')] if tags_div else ['']
            tags=[tag.replace('\n','').strip() for tag in tags]
            product_data_list = [
                product_id, category, product_name, price, main_image_url, additional_image_url,
                f"https://www.stellamccartney.com{product_details_url}", ', '.join(colors), ', '.join(tags)
            ]

            parsed_data.append(product_data_list)

        return parsed_data
class TomFordProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'tom_ford')
    def parse_product_blocks(self, soup,category):
        parsed_data = []
        column_names = [
            'product_id', 'product_name', 'product_url', 'price', 'images', 'quick_view_url','variation_count'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('div', class_='product-tile-wrap')
        for block_temp in product_blocks:
            block=block_temp.find('div',class_='product')
            product_id = block.get('data-pid', '')
            product_name = block.find('a', class_='link').text.strip() if block.find('a', class_='link') else ''
            product_url = block.find('a', class_='tile-image-container')['href'] if block.find('a', class_='tile-image-container') else ''
            product_url= f"https://www.tomfordfashion.com/{product_url}" if product_url else ''

            price_element = block.find('span', class_='value')
            price = price_element.text.strip() if price_element else ''

            image_elements = block.find_all('img', class_='loaded')
            images = [img['srcset'] for img in image_elements]

            quick_view_link = block.find('a', class_='quickview')
            quick_view_url = quick_view_link['href'] if quick_view_link else ''

            variation_element = block.find('div', class_='variation-count')
            variation_count = variation_element.text.strip() if variation_element else ''

            if "-" not in product_id:
                if block:
                    product_id = self.extract_product_id(str(block_temp))
                else:
                    product_id=''

            product_data = [
                product_id,
                product_name,
                product_url,
                price,
                ', '.join(images),
                quick_view_url,
                variation_count
            ]
            parsed_data.append(product_data)

        return parsed_data

    def extract_product_id(self,html_content):
        # Regular expression to find the specific script tag
        pattern = r'<script>window\._gdl=window\._gdl\|\|\[];window\._gdl\.push\((.*?)\);</script>'

        # Find all matches
        matches = re.findall(pattern, html_content)

        if matches:
            # Get the first match
            data_string = matches[0]

            # Convert the string to a Python dictionary
            data = json.loads(data_string)

            # Extract the product ID
            product_id = data['d']['id']

            return product_id

        return None
class OffWhiteProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'off_white')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'category', 'product_name', 'product_id', 'product_url', 'image_url', 'full_price', 'discount_price',
            'availability'
        ]
        parsed_data.append(column_names)
        main_block=soup.find("div",class_="css-1fb01a3 emnvohd7")
        product_blocks = main_block.find_all('li')

        for product in product_blocks:
            # Extract product name
            name_element = product.find('p', class_='css-1dw89jd e1i2jpfv10')
            product_name = name_element.get_text(strip=True) if name_element else ''

            # Extract product URL
            url_element = product.find('a', class_='css-1ym16s2 e1i2jpfv14')
            product_url = url_element.get('href') if url_element else ''

            # Extract product ID
            temp_id=product_url.split("-")[-1]
            product_id=temp_id
            # Extract image URL
            image_element = product.find('img', class_='css-wn0zwz er5xw931')
            image_url = image_element.get('src') if image_element else ''

            # Extract prices
            full_price = ''
            discount_price = ''
            availability = 'In stock'

            price_container = product.find('div', class_='e1i2jpfv9 css-1hril2i e1933l763')
            if price_container:
                full_price_element = price_container.find('span', class_='css-1go0pru e1933l761')
                discount_price_element = price_container.find('span', class_='css-uqjroe e1933l762')
                sale_price_element = price_container.find('span', class_='css-bks3r1 e1933l761')

                if full_price_element:
                    full_price = full_price_element.get_text(strip=True)
                if sale_price_element:
                    full_price = sale_price_element.get_text(strip=True)

                discount_price = discount_price_element.get_text(strip=True) if discount_price_element else ''

            # Check for sold-out status
            sold_out_element = product.find('div', class_='css-x9w7eo e1i2jpfv17')
            if sold_out_element and 'sold out' in sold_out_element.text.lower():
                availability = 'Sold out'

            product_data = [
                category,
                product_name,
                product_id,
                f"https://www.off---white.com{product_url}" if product_url else '',
                image_url,
                full_price,
                discount_price,
                availability
            ]
            parsed_data.append(product_data)

        return parsed_data
class IsabelMarantProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'isabel_marant')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'user_category', 'product_url', 'product_name', 'price', 'discounted_price', 'image_urls', 'sizes',
            'availability', 'label'
        ]
        parsed_data.append(column_names)

        product_items = soup.find_all('product-item', class_='product-item')

        for item in product_items:
            product_url = item.find('a', class_='product-item__aspect-ratio')['href'] if item.find('a',class_='product-item__aspect-ratio') else ''
            product_name = item.find('a', class_='product-item-meta__title').text.strip() if item.find('a',class_='product-item-meta__title') else ''

            price_element = item.find('span', class_='price--compare')
            discounted_price_element = item.find('span', class_='price--highlight')

            price = ''
            discounted_price = ''

            if price_element and discounted_price_element:
                price = price_element.text.strip()
                discounted_price = discounted_price_element.text.strip()
            else:
                # For non-discounted items
                regular_price_element = item.find('span', class_='price')
                if regular_price_element:
                    price = regular_price_element.text.strip()

            price = re.sub(r'[^\d.,]', '', price)
            discounted_price = re.sub(r'[^\d.,]', '', discounted_price)
            # Extract all image URLs
            image_elements = item.find_all('img')
            image_urls = [img.get('src') for img in image_elements if img.get('src')]

            # Extract sizes and availability
            sizes_list = item.find('ul', class_='plp-sizes-list')
            sizes = []
            availability = []
            if sizes_list:
                for li in sizes_list.find_all('li'):
                    sizes.append(li.text.strip())
                    availability.append('available' if 'not-available' not in li.get('class', []) else 'not available')

            # Extract labels
            label_elements = item.find_all('span', class_='label label--custom')
            labels = [label.text.strip() for label in label_elements if label.text.strip()]

            product_data = [
                category,
                product_url,
                product_name,
                price,
                discounted_price,
                ', '.join(image_urls),
                ', '.join(sizes),
                ', '.join(availability),
                ', '.join(labels)
            ]
            parsed_data.append(product_data)

        return parsed_data
class ChloeProductParser(WebsiteParser):
    def __init__(self, job_id):
        options = webdriver.ChromeOptions()
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
        self.driver = webdriver.Chrome(options=options)
        super().__init__(job_id, 'chloe')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'Product_ID','Cod10', 'Title', 'Price', 'position', 'category', 'macro_category', 'micro_category', 'macro_category_id', 'micro_category_id', 'color', 'color_id', 'product_price', 'discountedPrice', 'price_tf', 'discountedPrice_tf', 'quantity', 'coupon', 'is_in_stock', 'list', 'url', 'img_src', 'filename'
        ]
        parsed_data.append(column_names)
        articlesChloe = soup.find_all('article', {'class': 'item'})

        for articleChloe in articlesChloe:

            product_data = []
            imgSource = articleChloe.find('img')
            if imgSource:
                imgSource = imgSource['src']

            data_pinfo = articleChloe['data-ytos-track-product-data']

            a_url = articleChloe.find('a')
            if a_url:
                a_url = a_url['href']

            product_id=''
            product_id=self.extract_product_id(a_url)


            product_info = json.loads(data_pinfo)

            product_data.append(product_id)
            product_data.append(product_info['product_cod10'])
            product_data.append(product_info['product_title'])
            product_data.append(product_info['product_price'])
            product_data.append(product_info['product_position'])
            product_data.append(product_info['product_category'])
            product_data.append(product_info['product_macro_category'])
            product_data.append(product_info['product_micro_category'])

            product_data.append(product_info['product_macro_category_id'])
            product_data.append(product_info['product_micro_category_id'])
            product_data.append(product_info['product_color'])
            product_data.append(product_info['product_color_id'])
            product_data.append(product_info['product_price'])
            product_data.append(product_info['product_discountedPrice'])

            product_data.append(product_info['product_price_tf'])
            product_data.append(product_info['product_discountedPrice_tf'])
            product_data.append(product_info['product_quantity'])
            product_data.append(product_info['product_coupon'])
            product_data.append(product_info['product_is_in_stock'])
            product_data.append(product_info['list'])

            product_data.append(a_url)

            product_data.append(imgSource)
            product_data.append(category)
            parsed_data.append(product_data)
        return parsed_data
    def extract_product_id(self,product_url):
        self.driver.get(product_url)
        time.sleep(1)
        html=self.driver.page_source
        pid_text='Item code:'
        soup_pid=BeautifulSoup(html, 'html.parser') if html else ''
        if soup_pid:
            main_item = soup_pid.find('div', class_='itemdescription')
            if main_item:
                # Extract the Style ID
                style_id_text=main_item.text
                print(style_id_text)
                style_id = style_id_text.split(pid_text)[1].strip()
                print(f"Product ID: {style_id}")
                return style_id
            else:
                print(f'Product ID not found for url: {product_url}')
        else:
            print(f'Your URL is broken: {product_url}')
class MCMProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'mcm')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'user_category', 'product_id', 'product_url', 'product_name', 'price', 'discounted_price', 'image_urls',
            'availability'
        ]
        parsed_data.append(column_names)

        product_tiles = soup.find_all('a', class_='product-tile')

        for tile in product_tiles:
            product_id = tile.get('data-itemid', '')
            product_url = tile.get('href', '')
            product_name = tile.find('h2').text.strip() if tile.find('h2') else ''

            # Price and Discounted Price
            price_element = tile.find('span', class_='price-value')
            price = price_element.text.strip() if price_element else ''
            discounted_price_element = tile.find('span', class_='product-sales-price')
            discounted_price = discounted_price_element.find('span', class_='price-value').text.strip() if discounted_price_element else ''

            # Extract all image URLs
            image_elements = tile.find_all('source', media=True)
            image_urls = [img['srcset'].split()[0] for img in image_elements if 'srcset' in img.attrs]

            availability = tile.get('data-itemid', '')

            product_data = [
                category,
                product_id,
                product_url,
                product_name,
                price,
                discounted_price,
                ', '.join(image_urls),
                availability
            ]
            parsed_data.append(product_data)
        return parsed_data
class CultGaiaProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'cult_gaia')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'user_category', 'product_url', 'product_name', 'price', 'discounted_price', 'image_urls', 'tag'
        ]
        parsed_data.append(column_names)

        product_tiles = soup.find_all('div', class_='products__listing-details')

        for tile in product_tiles:
            product_url = tile.find('a')['href'] if tile.find('a') else ''
            product_name_element = tile.find('p', class_='s upper')
            product_name = product_name_element.contents[0].strip() if product_name_element else ''
            price = tile.find('p', class_='s upper').text.strip().split('$')[-1] if tile.find('p',
                                                                                              class_='s upper') else ''
            discounted_price = ''

            # Check for discounted price
            compare_price = tile.find('s', {'data-compare-price': ''})
            if compare_price and compare_price.previous_sibling:
                discounted_price = compare_price.previous_sibling.strip()

            # Extract all image URLs
            image_urls = []
            gallery = tile.find_previous_sibling('div', class_='products__listing-image')
            if gallery:
                image_elements = gallery.find_all('img')
                for img in image_elements:
                    srcset = img.get('srcset', '').split(',')
                    for src in srcset:
                        url = src.strip().split(' ')[0]
                        if url:
                            image_urls.append(url)

            # Extract tag
            tag = tile.find('p', class_='tag').text.strip() if tile.find('p', class_='tag') else ''
            product_data = [
                category,
                product_url,
                product_name,
                price,
                discounted_price,
                ', '.join(image_urls),
                tag
            ]
            parsed_data.append(product_data)

        return parsed_data
class GoldenGooseProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'golden_goose')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'user_category', 'product_id', 'product_url', 'product_name', 'price', 'discounted_price', 'image_urls'
        ]
        parsed_data.append(column_names)

        product_tiles = soup.find_all('li', class_='product-tile-container')

        for tile in product_tiles:
            product_div = tile.find('div', class_='product master')
            product_id = product_div['data-pid'] if product_div else ''
            product_url = tile.find('a', class_='js-product-tile_link')['href'] if tile.find('a', class_='js-product-tile_link') else ''
            product_name = tile.find('div', class_='pdp-link').find('a').text.strip() if tile.find('div', class_='pdp-link') and tile.find('div', class_='pdp-link').find('a') else ''
            price_element = tile.find('div', class_='price')
            price = price_element.find('span', class_='value').text.strip() if price_element and price_element.find('span', class_='value') else ''
            discounted_price = ''

            # Extract all image URLs
            image_urls = []
            picture_elements = tile.find_all('picture', class_='akamai-picture')
            for picture in picture_elements:
                source_elements = picture.find_all('source')
                for source in source_elements:
                    srcset = source.get('data-srcset', '').split(',')
                    for src in srcset:
                        url = src.strip().split(' ')[0]
                        if url:
                            image_urls.append(url)
                img_elements = picture.find_all('img')
                for img in img_elements:
                    url = img.get('data-src', '') or img.get('src', '')
                    if url:
                        image_urls.append(url)

            product_data = [
                category,
                product_id,
                product_url,
                product_name,
                price,
                discounted_price,
                ', '.join(image_urls)
            ]
            parsed_data.append(product_data)

        return parsed_data
class BalenciagaProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'balenciaga')
    def parse_product_blocks(self, soup, category):
        product_blocks = soup.find_all('article', class_='c-product')
        parsed_data = []

        column_names = [
            'data_pid', 'id', 'name', 'collection', 'productSMC', 'material', 'customization',
            'packshotType', 'brand', 'color', 'colorId', 'size', 'price', 'discountPrice',
            'coupon', 'subCategory', 'category', 'topCategory', 'productCategory', 'macroCategory',
            'microCategory', 'superMicroCategory', 'list', 'stock', 'productGlobalSMC', 'images', 'product_url'
        ]

        parsed_data.append(column_names)

        for block in product_blocks:
            product_data = []

            # Extract data-pid
            data_pid = block['data-pid']

            # Extract data-gtmproduct and parse JSON
            data_gtmproduct = block['data-gtmproduct']
            data_gtmproduct = html.unescape(data_gtmproduct)
            product_info = json.loads(data_gtmproduct)

            # Extract product images
            images = []
            image_container = block.find('div', class_='c-product__imageslider')
            if image_container:
                image_elements = image_container.find_all('img', class_='c-product__image')
                for img in image_elements:
                    images.append(img.get('src',''))
                    images.append(img.get('data-src',''))
                    images.append(img.get('srcset',''))
            images=list(set(images))
            images=[image for image in images if image != '' and "/on/demandware.static/" not in image]
            # Extract product URL
            product_url = ""
            product_url_component = block.find('a', class_='c-product__focus')
            if product_url_component:
                product_url = product_url_component['href']

            # Append the extracted information to the product_data list
            product_data.append(data_pid)
            product_data.append(product_info['id'])
            product_data.append(product_info['name'])
            product_data.append(product_info['collection'])
            product_data.append(product_info['productSMC'])
            product_data.append(product_info['material'])
            product_data.append(product_info['customization'])
            product_data.append(product_info['packshotType'])
            product_data.append(product_info['brand'])
            product_data.append(product_info['color'])
            product_data.append(product_info['colorId'])
            product_data.append(product_info['size'])
            product_data.append(product_info['price'])
            product_data.append(product_info['discountPrice'])
            product_data.append(product_info['coupon'])
            product_data.append(product_info['subCategory'])
            product_data.append(category)
            product_data.append(product_info['topCategory'])
            product_data.append(product_info['productCategory'])
            product_data.append(product_info['macroCategory'])
            product_data.append(product_info['microCategory'])
            product_data.append(product_info['superMicroCategory'])
            product_data.append(product_info['list'])
            product_data.append(product_info['stock'])
            product_data.append(product_info['productGlobalSMC'])
            product_data.append(', '.join(images))
            product_data.append(product_url)

            parsed_data.append(product_data)

        return parsed_data
class StoneIslandProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'stone_island')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        container_prods = soup.find('ul', {'class': 'products'})

        ##SELECT ALL BLOCKS FIRST

        articulos = container_prods.find_all('li', {'class': 'item'}) if container_prods else []

        #COLUMN NAMES
        column_names = [
            'category', 'product_title', 'product_price', 'product_discountedPrice', 'product_position',
            'product_cod10', 'product_brand', 'product_category', 'product_macro_category', 'product_micro_category',
            'product_macro_category_id', 'product_color', 'product_color_id', 'product_micro_category_id',
            'product_price_tf', 'product_discountedPrice_tf', 'product_quantity', 'product_coupon',
            'product_is_in_stock', 'a_href', 'img']
        parsed_data.append(column_names)

        for articulo in articulos:
            item_desc = articulo.find('div', {'class': 'item'})

            data_gtmproduct = item_desc.get('data-ytos-track-product-data','') if item_desc else ''
            data_gtmproduct = html.unescape(data_gtmproduct)
            product_info = json.loads(data_gtmproduct) if data_gtmproduct else ''
            if product_info:
              product_position = product_info.get('product_position','')
              product_cod10 = product_info.get('product_cod10','')

              product_brand = product_info.get('product_brand','')
              product_category = product_info.get('product_category','')
              product_macro_category = product_info.get('product_macro_category','')
              product_micro_category = product_info.get('product_micro_category','')
              product_macro_category_id = product_info.get('product_macro_category_id','')
              product_color = product_info.get('product_color','')
              product_color_id = product_info.get('product_color_id','')
              product_title = product_info.get('product_title','')

              product_price = product_info.get('product_price','')
              product_discountedPrice = product_info.get('product_discountedPrice','')
              product_micro_category_id = product_info.get('product_micro_category_id','')
              product_price_tf = product_info.get('product_price_tf','')
              product_discountedPrice_tf = product_info.get('product_discountedPrice_tf','')
              product_quantity = product_info.get('product_quantity','')
              product_coupon = product_info.get('product_coupon','')
              product_is_in_stock = product_info.get('product_is_in_stock','')
            else:
              product_position = ''
              product_cod10 = ''
              product_brand = ''
              product_category = ''
              product_macro_category = ''
              product_micro_category = ''
              product_macro_category_id = ''
              product_color = ''
              product_color_id = ''
              product_title = ''
              product_price = ''
              product_discountedPrice = ''
              product_micro_category_id = ''
              product_price_tf = ''
              product_discountedPrice_tf = ''
              product_quantity = ''
              product_coupon = ''
              product_is_in_stock = ''

            a_href = articulo.find('a', {'class': 'itemLink'})

            a_href = a_href.get('href','')

            img = articulo.find('img')
            img = img.get('src','')

            product_data = [
                category,
                product_title,
                product_price,
                product_discountedPrice,
                product_position,
                product_cod10,
                product_brand,
                product_category,
                product_macro_category,
                product_micro_category,
                product_macro_category_id,
                product_color,
                product_color_id,
                product_micro_category_id,
                product_price_tf,
                product_discountedPrice_tf,
                product_quantity,
                product_coupon,
                product_is_in_stock,
                a_href,
                img,
            ]
            parsed_data.append(product_data)

        return parsed_data
class EtroProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'etro')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'user_category', 'product_id', 'product_url', 'product_name', 'price', 'discounted_price', 'image_urls'
        ]
        parsed_data.append(column_names)

        product_tiles = soup.find_all('article', class_='producttile')

        for tile in product_tiles:
            product_id = tile['data-productid'] if 'data-productid' in tile.attrs else ''
            product_url = tile.find('a', class_='producttile-gallery-inner')['href'] if tile.find('a',
                                                                                                  class_='producttile-gallery-inner') else ''
            product_name = tile.find('h2', class_='producttile-name').text.strip() if tile.find('h2',
                                                                                                class_='producttile-name') else ''

            # Price elements
            price = ''
            discounted_price = ''
            price_element = tile.find('span', class_='price--full')
            discounted_price_element = tile.find('span', class_='price--current')

            if price_element and discounted_price_element:
                price = price_element.text.strip()
                discounted_price = discounted_price_element.text.strip()
            elif discounted_price_element:
                price = discounted_price_element.text.strip()

            # Extract all image URLs
            image_urls = []
            picture_elements = tile.find_all('picture')
            for picture in picture_elements:
                source_elements = picture.find_all('source')
                for source in source_elements:
                    srcset = source.get('data-srcset', '').split(',')
                    for src in srcset:
                        url = src.strip().split(' ')[0]
                        if url:
                            image_urls.append(url)
                img_elements = picture.find_all('img')
                for img in img_elements:
                    url = img.get('data-src', '') or img.get('src', '')
                    if url:
                        image_urls.append(url)

            product_data = [
                category,
                product_id,
                product_url,
                product_name,
                price,
                discounted_price,
                ', '.join(image_urls)
            ]
            parsed_data.append(product_data)

        return parsed_data
class BalmainProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'balmain')
    def parse_product_blocks(self, soup, category):

        parsed_data = []
        column_names = [
            'product_id', 'category', 'item_name', 'quantity', 'inventory_ats', 'price', 'full_price', 'discount_name',
            'pre_order', 'item_ean', 'item_master', 'item_category', 'item_category2', 'item_category3',
            'image_url', 'product_url'
        ]
        parsed_data.append(column_names)

        product_items = soup.find_all('div', class_='product-body')
        print(len(product_items))

        for item in product_items:
            full_price = None
            product_info = item.find('div', class_='product')
            if product_info:
                data_analytics = product_info['data-analytics']
                analytics_data = json.loads(html.unescape(data_analytics))
                product_data = analytics_data['product']
                product_tile = product_info.find('div', class_='product-tile')
                # print(product_tile)
                product_box = product_tile.find('div', class_='tile-box')
                price_info = product_box.find('div', class_='price')
                full_price_block = price_info.find('span', class_='strike-through list')
                if full_price_block:
                    full_price = full_price_block.find('span', class_='value')
                    full_price = full_price['content']

                images = [img['src'] for img in item.find_all('img') if img.get('src')]
                product_url = item.find('a', class_='tile-body')['href']
                images = list(set(images))
                images = [image for image in images if image != '' and "logo-sm" not in image]

                product_data_list = [
                    product_data.get('item_id', ''),
                    category,
                    product_data.get('item_name', ''),
                    product_data.get('quantity', ''),
                    product_data.get('inventory_ats', ''),
                    product_data.get('price', ''),
                    full_price,
                    product_data.get('discount_name', ''),
                    product_data.get('pre_order', ''),
                    product_data.get('item_ean', ''),
                    product_data.get('item_master', ''),
                    product_data.get('item_category', ''),
                    product_data.get('item_category2', ''),
                    product_data.get('item_category3', ''),
                    ", ".join(images),
                    str(product_url)
                ]

                parsed_data.append(product_data_list)

        return parsed_data
class VersaceProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'versace')
    def parse_product_blocks(self, soup, category):
        product_blocks = soup.select('.product-tile-show')
        parsed_data = []

        column_names = [
            'product_id', 'availability', 'category', 'image_url', 'list_price', 'name',
            'price', 'quantity', 'sale', 'sale_percentage', 'sale_price', 'sku',
            'stock', 'product_url', 'personalized', 'preorder', 'shoppable', 'promotional_code_name',
            'promotional_code_value', 'tax_included'
        ]
        parsed_data.append(column_names)

        for block in product_blocks:
            json_data_element = block.find('span', class_='analytics-product-data')
            if json_data_element:
                json_data = json_data_element.get('data-tracking-products', '')
                try:
                    product_info = json.loads(html.unescape(json_data))
                except json.JSONDecodeError:
                    continue  # Skip this block if JSON data is invalid or empty
            else:
                continue  # Skip if no json data element found

            product_data = [
                block['data-product-id'],
                product_info.get('product_availability'),
                product_info.get('product_category'),
                product_info.get('product_image'),
                product_info.get('product_list_price'),
                product_info.get('product_name'),
                product_info.get('product_price'),
                product_info.get('product_quantity'),
                product_info.get('product_sale'),
                product_info.get('product_sale_percentage'),
                product_info.get('product_sale_price'),
                product_info.get('product_sku'),
                product_info.get('product_stock'),
                block.find('a', class_='back-to-product-anchor-js')['href'],
                product_info.get('product_personalized'),
                product_info.get('product_preorder'),
                product_info.get('product_shoppable'),
                product_info.get('promotional_code_name'),
                product_info.get('promotional_code_value'),
                product_info.get('product_tax_included')
            ]

            parsed_data.append(product_data)

        return parsed_data
class FerragamoProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'ferragamo')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'product_id', 'product_url', 'product_name', 'price','old_price', 'image_urls', 'label'
        ]
        parsed_data.append(column_names)

        product_items = soup.find_all('li', class_='r23-grid--list-plp__item')

        for item in product_items:
            product_id = item.find('button', class_='r23-grid--list-plp__item__product-wishlist')['data-partnumber']
            product_link = item.find('a')
            product_url = product_link['href']
            product_name = item.find('div', class_='r23-grid--list-plp__item__info__product-name').text.strip()
            product_price = item.find('span', class_='r23-grid--list-plp__item__info__product-price-new').text.strip()
            old_product_price = item.find('s',class_='r23-grid--list-plp__item__info__product-price-old').text.strip()
            label = item.find('span', class_='r23-grid--list-plp__item__st').text if item.find('span', class_='r23-grid--list-plp__item__st') else ''

            images = item.find_all('img', class_='r23-grid--list-plp__item__img')
            image_urls = [img.get('data-src', 'src') for img in images]

            product_data = [
                product_id,
                f"https://www.ferragamo.com{product_url}",
                product_name,
                product_price,
                old_product_price,
                ', '.join(image_urls),
                label
            ]
            parsed_data.append(product_data)

        return parsed_data
class BurberryProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'burberry')
    def parse_product_blocks(self, soup, category):

        product_blocks = soup.select('a.product-card-v2-anchor, a.redesigned-product-card__link')
        print(len(product_blocks))
        parsed_data = []

        column_names = [
            'category', 'name', 'product_id', 'product_url', 'image_url', 'full_price', 'discount_price', 'tag(s)']

        parsed_data.append(column_names)

        base_url = soup.find('meta', {'property': 'org:url'})
        if base_url:
            base_url = base_url['content']
        else:
            base_url = "https://us.burberry.com/"

        for product in product_blocks:
            name = product.select_one('.product-card-v2-title, .product-card-content__title').get_text(strip=True) if product.select_one('.product-card-v2-title, .product-card-content__title') else ''

            product_link = urljoin(base_url, product['href'])

            product_id = re.search('\-p(\d+)/?$', product_link, flags=re.I)
            product_id = product_id.group(1) if product_id else "unavailable"

            price = product.select_one('.product-card-price__current-price, .product-card-v2-price__current').get_text(
                strip=True) if product.select_one('.product-card-price__current-price, .product-card-v2-price__current') else ''

            discount_price = ""  # empty for now and can be added later if found

            image_url = product.select_one('.redesigned-product-card__picture img, .product-card-v2-carousel-container__media__picture img')
            image_url=image_url['src'] if image_url else ""

            tags = product.select_one('.product-card-labels__flag, .product-card-v2-carousel-labels__label')
            tags = tags.get_text(strip=True) if tags else ""

            product_data = [
                category,
                name,
                product_id,
                product_link,
                image_url,
                price,
                discount_price,
                tags
            ]

            parsed_data.append(product_data)

        return parsed_data
class KenzoProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'kenzo')
    def parse_product_blocks(self, soup, category):

        product_blocks = soup.select("div[is='m-product-tile']")
        print(len(product_blocks))
        parsed_data = []

        column_names = [
            'category', 'name', 'product_id', 'product_url', 'image_url', 'other_images', 'full_price',
            'discount_price', 'stock', 'color', 'tag(s)']

        parsed_data.append(column_names)

        base_url = soup.find('meta', {'property': 'org:url'})
        if base_url:
            base_url = base_url['content']
            base_url = urlunparse(urlparse(base_url)[:3] + ('', '', ''))
        else:
            base_url = "https://www.kenzo.com/"

        for product in product_blocks:

            name = product.select_one('a[class*="title t-body-bold t-plain"]').get_text(strip=True)

            product_link = product.select_one('a[class*="title t-body-bold t-plain"]')
            product_link = urljoin(base_url, product_link['href'])

            product_id = product['data-pid'] if product.get('data-pid') else "unavailable"

            price = product.select_one('.prices .price-sales, .prices .price').get_text(strip=True).lower()
            price = re.sub('price\s+reduced\s+from', '', price, flags=re.I | re.S).replace('to', '').strip()

            discount_price = product.select_one('.prices .reduced-price')
            discount_price = discount_price.get_text(strip=True) if discount_price else ""

            all_images = []
            for img in product.select("[is='m-tile-images'] img"):
                image_url = self.get_biggest_image_srcset(img)
                if image_url and image_url not in all_images:
                    all_images.append(image_url)

            image_url = all_images[0] if all_images else ""

            other_images = ' , '.join(all_images[1:]) if len(all_images) > 1 else ""

            stock_status = product.select_one('.stock-state')
            stock_status = stock_status.get('aria-label') or stock_status.get_text(strip=True) if stock_status else ""

            colors_ul = product.select_one('ul[is="m-tile-color"]')
            if colors_ul:
                colors = [li.find('button')['aria-label'] for li in colors_ul.select('li') if li.find('button')]
                colors = [re.sub('color\s+product', '', color, flags=re.I).strip() for color in colors]
                colors = [color for color in colors if color]
                if colors and colors_ul.select_one('.more-color'):
                    colors.append(colors_ul.select_one('.more-color').get_text(strip=True) + " more")
                colors = ', '.join(colors)
            else:
                colors = ""

            tags = product.select_one('[is="m-product-tag"]')
            tags = tags.get_text(strip=True) if tags else ""

            product_data = [
                category,
                name,
                product_id,
                product_link,
                image_url,
                other_images,
                price,
                discount_price,
                stock_status,
                colors,
                tags
            ]

            parsed_data.append(product_data)

        return parsed_data

    def get_biggest_image_srcset(self, img_tag):
        if not img_tag or 'srcset' not in img_tag.attrs:
            if img_tag and img_tag.get('src'):
                return img_tag['src']
            return None

        srcset = img_tag['srcset']
        srcset_entries = srcset.split(',')

        images = []
        for entry in srcset_entries:
            parts = entry.strip().split(' ')
            url = parts[0]
            if len(parts) > 1:
                descriptor = parts[1]
                if descriptor.endswith('w'):
                    width = int(descriptor[:-1])
                    images.append((width, url))
                elif descriptor.endswith('x'):
                    density = float(descriptor[:-1])
                    width = density * 1920
                    images.append((width, url))
                else:
                    continue
            else:
                images.append((1920, url))

        if not images:
            return None

        biggest_image_url = max(images, key=lambda x: x[0])[1]
        return biggest_image_url
class JimmyChooProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'jimmy_choo')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'product_id', 'category', 'product_name', 'full_price', 'discount_price', 'colors', 'article_id',
            'product_url', 'color_code', 'image_urls', 'product_type', 'currency', 'description'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('li', class_='js-grid-tile')

        for product in product_blocks:
            details_element = product.find('div', class_='js-googledatalayer-data-productTile')
            details_dict = details_element.get('data-value') if details_element else ''
            if details_dict:
                details_dict = json.loads(details_dict)
                product_id = details_dict.get('id', '')
                category = details_dict.get('subcategory', '')
                product_name = details_dict.get('name', '')
                full_price = details_dict.get('base_price', '')
                discount_price = details_dict.get('price', '')
                colors = details_dict.get('color', '')
                article_id = details_dict.get('attributes', {}).get('Article_ID', '')
                product_url = details_dict.get('url', '')
                master_id = details_dict.get('masterID', '').replace('_S', '')
                color_code = product_id.replace('_S', '').replace(master_id, '')
                image_urls = details_dict.get('image_urls', [])
                product_type = details_dict.get('type', '')
                currency = details_dict.get('currency', '')
                description = details_dict.get('description', '')
            else:
                # Extract product ID
                product_id = product.get('data-itemid', '')

                # Extract product name
                name_element = product.find('h2', class_='product-name')
                product_name = name_element.get_text(strip=True) if name_element else ''

                # Extract product URL
                url_element = name_element.find('a') if name_element else None
                product_url = url_element.get('href') if url_element else ''

                # Extract image URLs
                image_urls = []
                images = product.find_all('img', class_='product-image-slider-item')
                for img in images:
                    image_urls.append(img.get('data-src', img.get('src', '')))

                # Extract prices
                full_price = ''
                discount_price = ''

                # Look for price in the price range element
                price_element = product.find('div', class_='price-range')
                if price_element:
                    full_price_tag = price_element.find('span', class_='js-st-price')
                    full_price = full_price_tag.get_text(strip=True) if full_price_tag else ''

                    discount_price_tag = price_element.find('span', class_='js-sl-price')
                    discount_price = discount_price_tag.get_text(strip=True) if discount_price_tag else ''
                # Look for prices in the standard-price element if not found in price-range
                if not full_price:
                    standard_price_element = product.find('div', class_='standart-price')
                    if standard_price_element:
                        full_price_tag = standard_price_element.find('span', class_='js-st-price')
                        full_price = full_price_tag.get_text(strip=True) if full_price_tag else ''
                if not discount_price:
                    top = product.find('div', class_='price-range')
                    if top:
                        discount_price_tag = top.find('span', class_='product-sale-price-wrap')
                        if discount_price_tag:
                            discount_price_tag = discount_price_tag.find('span', class_="js-sl-price")
                            discount_price = discount_price_tag.get_text(strip=True)

                # Extract Colors

                color_elements = product.find_all('img', class_='js-defer-image')
                colors = []
                for color in color_elements:
                    colors.append(color.get('alt'))
                colors = ', '.join(colors)

                article_id = ''
                product_type = ''
                currency = ''
                description = ''
                color_code = ''
                product_type = ''

            product_data = [
                product_id,
                category,
                product_name,
                full_price,
                discount_price,
                colors,
                article_id,
                product_url,
                color_code,
                "|".join(image_urls),
                product_type,
                currency,
                description
            ]
            parsed_data.append(product_data)

        return parsed_data
class BrunelloCucinelliProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'brunello_cucinelli')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        column_names = [
            'user_category', 'product_url', 'product_id', 'product_name', 'price', 'discounted_price', 'image_urls',
            'sizes',
            'availability', 'label'
        ]
        parsed_data.append(column_names)

        product_items = soup.find_all('div', class_='product')

        for item in product_items:
            product_url_element_list = list(item.find_all('a', class_='js-pdp-variant-link'))
            try:
                product_url_element = product_url_element_list[0] if product_url_element_list[0] else ''
                product_name_element = product_url_element_list[1] if product_url_element_list[1] else ''
            except:
                product_url_element = ''
                product_name_element = ''
            product_url = product_url_element['href'] if product_url_element else ''
            product_id = product_url_element['data-pid'] if product_url_element else ''
            product_name = product_name_element.get_text() if product_name_element else ''
            product_name = self.clean_text(product_name)


            price_element = item.find('span', class_='price')
            discounted_price_element = item.find('span', class_='sales cc-sales')

            price = ''
            discounted_price = ''

            if price_element and discounted_price_element:
                price = price_element.find('span', class_='value').text.strip() if price_element.find('span',
                                                                                                      class_='value') else ''
                discounted_price = discounted_price_element.find('span',
                                                                 class_='value').text.strip() if discounted_price_element.find(
                    'span', class_='value') else ''
            else:
                regular_price_element = item.find('span', class_='cc-price-text')
                if regular_price_element:
                    price = regular_price_element.text.strip()

            # Clean up the price and discounted price
            price = re.sub(r'[^\d.,]', '', price)
            discounted_price = re.sub(r'[^\d.,]', '', discounted_price)

            # Extract all image URLs
            image_elements = item.find_all('img')
            image_urls = [img.get('src') for img in image_elements if img.get('src')]

            # Extract sizes and availability
            sizes_list = item.find_all('li', class_='cc-size-container')
            sizes = []
            availability = []
            if sizes_list:
                for li in sizes_list:
                    size_text = li.find('label').text.strip() if li.find('label') else ''
                    sizes.append(size_text)
                    availability.append(
                        'available' if 'cc-size-no-available' not in li.get('class', []) else 'not available')

            # Extract labels (if any)
            label_elements = item.find_all('span', class_='cc-product-tag')
            labels = [label.text.strip() for label in label_elements if label.text.strip()]

            product_data = [
                category,
                product_url,
                product_id,
                product_name,
                price,
                discounted_price,
                ', '.join(image_urls),
                ', '.join(sizes),
                ', '.join(availability),
                ', '.join(labels)
            ]
            parsed_data.append(product_data)

        return parsed_data

    def clean_text(self,text):
        text = text.split('\$')[0].split('€')[0]
        return text.strip()
class DSquaredProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'dsquared')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        container_prods = soup.find('div', {'id': 'productgrid'})
        articulos = container_prods.find_all('section')

        ##SELECT ALL BLOCKS FIRST

        column_names = [
            'data_id', 'name_prod', 'product_originalprice_tf', 'product_discount_tf', 'product_currentprice_tf',
            'product_instock', 'product_breadcrumb_label', 'url', 'imgs']
        parsed_data.append(column_names)

        for articulo in articulos:
            data_id = articulo.get('data-id', '')
            name_prod = articulo.get('aria-label', '')

            info_data = articulo.get('data-tc-analytics', '')
            info_data = html.unescape(info_data)
            info_data = json.loads(info_data) if info_data else None
            if info_data:
                product_originalprice_tf = info_data.get('product_originalprice_tf', '')

                product_discount_tf = info_data.get('product_discount_tf', '')
                product_currentprice_tf = info_data.get('product_currentprice_tf', '')

                product_instock = info_data.get('product_instock', '')
                product_breadcrumb_label = info_data.get('product_breadcrumb_label', '')
                url = info_data.get('product_url_page', '')

                imgs = articulo.find_all('img')
                imgs_list = []
                for img in imgs:
                    img_src = img.get('src', '')
                    imgs_list.append(img_src)

                imgs = ", ".join(imgs_list)
                product_data = [
                    data_id,
                    name_prod,
                    product_originalprice_tf,
                    product_discount_tf,
                    product_currentprice_tf,
                    product_instock,
                    product_breadcrumb_label,
                    url,
                    imgs,
                ]
                parsed_data.append(product_data)

        return parsed_data
class CelineProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'celine')
    def parse_product_blocks(self, soup, category):
        parsed_data = []
        container_prods = soup.find_all('div', class_='m-product-listing')

        column_names = [
            'product_id','color_id', 'product_name', 'product_price','currency', 'product_color', 'category', 'image_urls','product_url'
        ]
        parsed_data.append(column_names)

        for item in container_prods:
            product_id = item.get('data-id', '')
            color_id=product_id.split('.')[1] if product_id and '_' not in product_id  else ''
            imgs = item.find_all('img')
            imgs_list = []
            for img in imgs:
                img_src = img.get('data-lazy-src', '') or img.get('src', '')
                if img_src:
                    img_src=img_src.split("?")[0]
                imgs_list.append(img_src)

            image_urls = ", ".join(imgs_list)

            meta_div = item.find('div', class_='m-product-listing__meta')
            link = item.find('a', href=True)
            product_url = link['href']
            category_internal=link.get('category','')
            if meta_div:
                meta_title_div = meta_div.find('div', class_='m-product-listing__meta-title')
                product_name = meta_title_div.get_text(strip=True) if meta_title_div else ''

                color_span = meta_div.find('span', class_='a11y')
                product_color = color_span.get_text(strip=True) if color_span else ''
                product_color=product_color.split("; ")[1] if product_color else ''

                price_strong = meta_div.find('strong', class_='f-body--em')
                product_price = price_strong.get_text(strip=True) if price_strong else ''
                if product_price:
                    price=product_price.split(' ')[0]
                    currency=product_price.split(' ')[1]
                else:
                    price = ''
                    currency = ''
            else:
                product_name = ''
                product_color = ''
                price = ''
                currency=''

            product_data = [
                product_id,
                color_id,
                product_name,
                price,
                currency,
                product_color,
                category_internal,
                image_urls,
                product_url
            ]
            parsed_data.append(product_data)

        return parsed_data
class MarniProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'marni')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'product_id', 'product_name', 'original_price', 'discounted_price',
            'category', 'image_urls', 'product_url'
        ]
        parsed_data.append(column_names)

        items = soup.find_all('div', class_='col-12 col-md-3 single-element show-second-image')

        for item in items:
            product_id = item.find('div', class_='product').get('data-pid', '')
            product_name = item.find('div', class_='single-element-content-detail-description').find('a').text.strip()
            product_url = item.find('div', class_='single-element-content-detail-description').find('a').get('href', '')
            category = item.get('data-category', '')

            # Extract prices
            price_div = item.find('div', class_='price')
            original_price = price_div.find('span', class_='strike-through list')
            discounted_price = price_div.find('span', class_='sales')

            if original_price:
                original_price = original_price.find('span', class_='value').text.strip()
                discounted_price = discounted_price.find('span',
                                                         class_='value').text.strip() if discounted_price else ''
            else:
                original_price = discounted_price.find('span',
                                                       class_='value').text.strip() if discounted_price else ''
                discounted_price = ''
            original_price = original_price.replace(r'\n\n- Original Price', '') if original_price else ''
            discounted_price = discounted_price.replace(r'\n\n- Discounted Price', '') if discounted_price else ''
            # Extract image URLs
            image_divs = item.find_all('img', class_='ls-is-cached lazyloaded')
            image_urls = []
            for img in image_divs:
                srcset = img.get('srcset', '')
                if srcset:
                    # Extract all URLs from srcset
                    urls = re.findall(r'(https?://\S+)(?=\s\d+w)', srcset)
                    # Add the largest image (last URL) to the list
                    if urls:
                        image_urls.append(urls[-1])

            product_data = [
                product_id,
                product_name,
                original_price,
                discounted_price,
                category,
                ', '.join(image_urls),
                product_url
            ]
            parsed_data.append(product_data)

        return parsed_data
class PradaProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'prada')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'product_id', 'product_name', 'price',
            'category', 'image_urls', 'product_url'
        ]
        parsed_data.append(column_names)

        items = soup.find_all('li', class_='w-full h-auto lg:h-full')

        for item in items:
            product_card = item.find('article', class_='product-card')
            if not product_card:
                continue

            product_name = product_card.find('h3', class_='product-card__name').text.strip() if product_card.find('h3', class_='product-card__name') else ''
            product_url = product_card.find('a', class_='product-card__link').get('href', '') if product_card.find('a', class_='product-card__link') else ''
            category = category

            price = product_card.find('p', class_='product-card__price--new').text.strip() if product_card.find('p', class_='product-card__price--new') else ''

            # Extract image URLs
            image_divs = product_card.find_all('img', class_='product-card__picture')
            image_urls = []
            for img in image_divs:
                srcset = img.get('data-srcset', '')
                if srcset:
                    first_url = srcset.split()[0]
                    if first_url:
                        image_urls.append(first_url)

            # Extract colors
            colors = []
            color_dots = product_card.find_all('span', class_='w-3 h-3 rounded-full absolute border border-solid border-general-divider')
            for dot in color_dots:
                style = dot.get('style', '')
                color = style.split('background: ')[1][:-1] if 'background: ' in style else ''
                colors.append(color)
            product_id=product_url.split("/")[-1]
            product_data = [
                product_id,
                product_name,
                price,
                category,
                '| '.join(image_urls),
                product_url
            ]
            parsed_data.append(product_data)

        return parsed_data
class ValentinoProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'valentino')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'product_id', 'product_name', 'price', 'discount_price' 'category',
            'image_urls', 'product_url', 'colors', 'status'
        ]
        parsed_data.append(column_names)

        items = soup.find_all('li', class_='productCard-wrapper')

        for item in items:
            product_id = item.get('data-product-sku', '')
            product_name = item.get('data-product-name', '')
            price = item.get('data-full-price', '')
            discount_price = item.get('data-price', '')
            category = category
            status = item.get('data-status', '')
            color_description = item.get('data-color-description', '')

            # Extract product URL
            product_link = item.find('a', class_='productCard__image')
            product_url = product_link.get('href', '') if product_link else ''

            # Extract image URLs from both picture classes
            image_urls = []
            image_containers = item.find_all('picture')
            for container in image_containers:
                img = container.find('img')
                if img:
                    srcset = img.get('data-srcset', '').split(', ')
                    if srcset:
                        # Get the highest resolution image
                        image_urls.append(srcset[-1].split(' ')[0])

            product_data = [
                product_id,
                product_name,
                price,
                discount_price,
                category,
                ', '.join(image_urls),
                product_url,
                color_description,
                status
            ]
            parsed_data.append(product_data)

        return parsed_data
class JacquemusProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'jacquemus')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'product_id', 'color_id', 'product_name', 'price', 'discount_price', 'category',
            'image_urls', 'product_url', 'colors', 'status'
        ]
        parsed_data.append(column_names)

        items = soup.find_all('div', class_='product__tile')

        for item in items:
            # Extract product ID and other details from data-gtmdata attribute
            gtm_data = item.find('input', class_='gtmdata')
            if gtm_data:
                data = gtm_data.get('data-gtmdata', '')
                data = json.loads(data.replace('&quot;', '"')) if data else {}
                product_id = data.get('id', '')
                product_name = data.get('name', '')
                price = data.get('price', '0.0')
                category_internal=data.get('category','')
                color_description = data.get('color', '')
                status = 'in stock' if data.get('in_stock', 0) == 1 else 'out of stock'
            else:
                product_id = ''
                product_name = ''
                price = '0.0'
                color_description = ''
                status = ''
                category_internal=''

            if not category_internal:
                category_internal=category

            # Extract discount price
            price_element=item.find('div', class_='product__price')
            discount_price_element=price_element.find('span', class_='product__price__value -saled')
            discount_price=discount_price_element.text.strip() if discount_price_element else ''

            # Extract category
            category = category

            # Extract product URL
            product_title = item.find('a', class_='product__title__content')
            product_url = product_title.get('href', '') if product_title else ''

            # Extract image URLs
            image_urls = []
            images = item.find_all('img', class_='product__visuals__content')
            for img in images:
                main_image = img.get('src', '')
                image_urls.append(main_image)
                srcset = img.get('srcset', '').split(', ')
                for src in srcset:
                    image_urls.append(src.split(' ')[0])

            # Extract colors
            colors = []
            color_elements = item.find_all('div', class_='product__swatches__attributes__value')
            for color_element in color_elements:
                color_span = color_element.find('span')
                if color_span:
                    colors.append(color_span.text.strip())
            color_id=product_id.split('-')[-1]
            product_data = [
                product_id,
                color_id,
                product_name,
                price,
                discount_price,
                category_internal,
                ', '.join(image_urls),
                product_url,
                ', '.join(colors),
                status
            ]
            parsed_data.append(product_data)

        return parsed_data
class LouboutinProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'louboutin')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'product_id', 'product_name', 'price', 'category',
            'image_urls', 'product_url','color', 'material', 'season'
        ]
        parsed_data.append(column_names)

        rows=soup.find_all('div', class_='row-product')
        for row in rows:
            items = row.find_all('div', class_='product-item-info')
            for item in items:
              url_piece=item.find('a',class_="product-item-link")
              product_url=url_piece.get('href') if url_piece else ''
              price=item.find('span', class_='price')
              price=price.text.strip() if price else ''
              name=item.find('p', class_='m-0')
              name=name.text.strip() if name else ''

              # Extract image URLs
              image = item.find('img', class_='photo')
              image=image.get('src','') if image else ''

              product_id,color,material,season=self.extract_product_page_info(product_url)


              product_data = [
                  product_id,
                  name,
                  price,
                  category,
                  image,
                  product_url,
                  color,
                  material,
                  season
              ]
              parsed_data.append(product_data)

        return parsed_data
    def extract_product_page_info(self, product_url):
        print(f"Processing URL: {product_url}")
        html=self.open_link(product_url)
        pid_text = 'Reference :'
        color_text='Color :'
        material_text='Material :'
        season_text='Collection :'
        soup_pid=BeautifulSoup(html, 'html.parser') if html else ''
        if soup_pid:
            details_item=soup_pid.find('div',class_='additional-attributes-wrapper')
            p_items = details_item.find_all('p') if details_item else None
            if not p_items:
                return '', '', '', ''
            style_id_item=''
            color_item=''
            material_item=''
            season_item=''
            for item in p_items:
              if pid_text in item.text:
                style_id_item=item.text.strip()
              if color_text in item.text:
                color_item=item.text.strip()
              if material_text in item.text:
                material_item=item.text.strip()
              if season_text in item.text:
                season_item=item.text.strip()
            # Extract the Style I
            print(f"This is the item that contains style ID: {style_id_item}")
            style_id_list=style_id_item.split(pid_text) if style_id_item else []
            style_id = style_id_item.split(pid_text)[1].strip() if len(style_id_list)>1 else ''
            print(f"Product ID: {style_id}")

            # Extract the color
            print(f"This is the item that contains color: {color_item}")
            color_list=color_item.split(color_text) if color_item else []
            color = color_item.split(color_text)[1].strip() if len(color_list)>1 else ''
            print(f"Color: {color}")

            # Extract the material
            print(f"This is the item that contains material: {material_item}")
            material_list=material_item.split(material_text) if material_item else []
            material = material_item.split(material_text)[1].strip() if len(material_list)>1 else ''
            print(f"Material: {material}")

            # Extract the season
            print(f"This is the item that contains season: {season_item}")
            season_list=season_item.split(season_text) if season_item else []
            season = season_item.split(season_text)[1].strip() if len(season_list)>1 else ''
            print(f"Season: {season}")
            return style_id, color, material, season

        else:
            print(f'Your URL is broken: {product_url}')
            return '', '', '', ''
class PalmAngelsProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'palm_angels')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'product_id', 'product_name', 'price', 'category',
            'image_urls', 'product_url', 'sale_price', 'original_price', 'discount'
        ]
        parsed_data.append(column_names)

        items = soup.find_all('div', class_='css-u7k64p')  # Targeting the product card class

        for item in items:
            # Extract product URL from product title link
            product_title = item.find('a', class_='css-1kcohcr')
            product_url = product_title.get('href', '') if product_title else ''
            product_url=f"https://www.palmangels.com{product_url}"

            product_id=product_url.split("-")[-1]



            # Extract product name from product title text
            product_name_element = item.find('h2', class_='css-869f5i')
            product_name = product_name_element.text.strip() if product_name_element else ''

            # Extract image URLs from image sources
            image_urls = []
            images = item.find_all('img', class_='css-x5s4ri')
            for img in images:
                image_urls.append(img.get('src', ''))

            # Extract price details
            price_container = item.find('div', class_='css-1vwkltk')  # Find the price container
            if price_container:
                # Check for sale price first (assuming it's displayed prominently)
                sale_price_element = price_container.find('span', class_='css-nh3e0y')
                if sale_price_element:  # If sale price exists, use that and original price
                    sale_price = sale_price_element.text.strip() if sale_price_element else ''
                    original_price_element = price_container.find('span', class_='css-2droiu')
                    original_price = original_price_element.text.strip() if original_price_element else ''
                    discount_element = price_container.find('span', class_='css-hledai')
                    discount = discount_element.text.strip() if discount_element else ''
                else:  # If no sale price, use the regular price
                    original_price_element = price_container.find('span', class_='css-ltnzhu')
                    original_price = original_price_element.text.strip() if original_price_element else ''
                    sale_price = ''
                    discount = ''
            else:
                sale_price = ''
                original_price = ''
                discount = ''

            # Colors are not explicitly available in this HTML structure


            product_data = [
                product_id,  # Assuming product ID is not available
                product_name,
                original_price,  # Use sale price if available
                category,
                ', '.join(image_urls),
                product_url,
                sale_price,
                original_price,
                discount
            ]
            parsed_data.append(product_data)

        return parsed_data
class MooseKnucklesProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'moose_knuckles')

    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'product_id', 'product_name', 'price', 'category',
            'image_urls', 'product_url', 'colors', 'sale_price', 'original_price'
        ]
        parsed_data.append(column_names)

        items = soup.find_all('li', class_='ais-InfiniteHits-item')  # Target product blocks

        for item in items:
            product_link = item.find('a', class_='productimg-link')
            product_url = product_link.get('href', '') if product_link else ''

            product_name_element = item.find('span', title=True)
            product_name = product_name_element.text.strip() if product_name_element else ''

            image_element = item.find('img', class_='product-img')
            image_url = image_element.get('src', '') if image_element else ''

            colors = []
            color_divs = item.find_all('div', class_='hit-color')
            for color_div in color_divs:
                color_img = color_div.find('img')
                color_text = color_img.get('alt', '').strip() if color_img else ''
                colors.append(color_text)

            price_container = item.find('div', class_='product-price')
            price_span = price_container.find('span') if price_container else None
            price_text = price_span.text.strip() if price_span else ''

            try:
                sale_price = price_text.split("$")[2]
            except:
                sale_price = ''

            original_price = price_text.split("$")[1] if len(price_text.split("$")) > 1 else ''
            price = original_price  # Overwrite price to original_price

            product_id = product_url.split("-")[-1]
            if not product_id.startswith("m"):
                # Fallback if the last part doesn’t start with 'm'
                product_id = product_url.split("-")[-2]

            product_data = [
                product_id,
                product_name,
                price,
                category,
                image_url,
                product_url,
                ', '.join(colors),
                sale_price,
                original_price
            ]
            parsed_data.append(product_data)

        return parsed_data
class AcneStudiosProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'acne_studios')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'product_id', 'product_name', 'price', 'category',
            'image_urls', 'product_url', 'colors', 'sale_price', 'original_price', 'discount'
        ]
        parsed_data.append(column_names)

        # Target the tile class containing product information
        items = soup.find_all('div', class_='tile tile--span-4 tile--has-link product-tile')

        for item in items:
            # Extract product URL from product title link
            product_title_link = item.find('a', class_='tile__link')
            product_url = product_title_link.get('href', '') if product_title_link else ''

            # Extract product name from product title text
            product_name_element = item.find('div', class_='product-tile__name')
            product_name = product_name_element.find('a').text.strip() if product_name_element else ''
            product_id=None
            # Extract image URLs from image sources
            image_urls = []
            images = item.find_all('img', class_='lazyautosizes')
            for image in images:
                image_url = image.get('data-src', '')
                if not product_id:
                    product_id=image.get('alt','')
                    product_id=product_id.split(',')[0]
                    product_id = product_id.split(' ')[0]
                if image_url:
                    image_urls.append(image_url)

            # Extract price details
            price_container = item.find('div', class_='product-tile__price text-mask---small-down')
            if price_container:
                original_price = price_container.text.strip()
                sale_price = ''
                discount = ''
            else:
                original_price = ''
                sale_price = ''
                discount = ''

            # Extract colors from color list (assuming 'text-mask color--gray text--no-case' contains color info)
            colors = []
            color_info = item.find('ul')
            if color_info:
                color_items = color_info.find_all('li')
                for color_item in color_items:
                    colors.append(color_item.text.strip())
            length=len(product_id) if product_id else ''
            if length != 16 or not product_id:
                product_id = self.extract_product_id(product_url)
            product_data = [
                product_id,
                product_name,
                original_price,
                category,
                ','.join(image_urls),
                product_url,
                ','.join(colors),
                sale_price,
                original_price,
                discount
            ]
            parsed_data.append(product_data)

        return parsed_data
    def extract_product_id(self,product_url):
        html=self.open_link(product_url)
        pid_text = 'Style ID:'
        soup_pid=BeautifulSoup(html, 'html.parser') if html else ''
        if soup_pid:
            style_id_item = soup_pid.find('li', string=lambda text: pid_text in text if text else False)

            if style_id_item:
                # Extract the Style ID
                style_id = style_id_item.text.split(pid_text)[1].strip()
                print(f"Product ID: {style_id}")
                return style_id
            else:
                print("Product ID not found.")
        else:
            print(f'Your URL is broken: {product_url}')
class TheRowProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'the_row')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'product_id', 'product_name', 'price', 'category',
            'image_urls', 'product_url', 'sale_price', 'status'
        ]
        parsed_data.append(column_names)

        # Target the product container class
        items = soup.find_all('div', class_='ProductItem')

        for item in items:
            product_id=None
            # Extract product URL from product title link
            product_title_link = item.find('a', class_='ProductItem__ImageWrapper ProductItem__ImageWrapper--withAlternateImage')
            product_url = product_title_link.get('href', '') if product_title_link else ''
            product_url= 'https://www.therow.com' + product_url
            # Extract product name from product title text
            product_name_element = item.find('h3', class_='ProductItem__Title Heading')
            product_name = product_name_element.text.strip() if product_name_element else ''

            # Extract image URLs from image sources (considering both main and alternate images)
            image_urls = []
            images = item.find_all('img', class_=['ProductItem__Image ProductItem__Image--alternate', 'ProductItem__Image'])
            for img in images:
                image_urls.append(img.get('src', ''))
                if not product_id:
                    pid_temp = img.get('src', '')
                    product_id=pid_temp.split('/')[-1].split('_')[0]
            if len(product_id)<10:
                product_id=self.extract_product_id(product_url)

            image_urls =[x for x in image_urls if x.strip()]
            # Extract price details
            price_container = item.find('div', class_='ProductItem__PriceList Heading')  # Find the price container
            if price_container:
                price_element = price_container.find('span', class_='ProductItem__Price Price Price--highlight Text--subdued')
                price = price_element.text.strip() if price_element else ''
                sale_price_element = price_container.find('span', class_='ProductItem__Price Price Price--compareAt Text--subdued')
                sale_price = sale_price_element.text.strip() if sale_price_element else ''
                if sale_price:
                  original_price=sale_price
                  sale_price=price
                else:
                    original_price = price
            else:
                sale_price = ''
                original_price = ''

            # Check for sold-out indication
            sold_out_label = item.find('span', class_='ProductItem__Label ProductItem__Label--soldOut Heading Text--subdued')
            is_sold_out = 'Yes' if sold_out_label else 'No'

            # Colors are not explicitly available in this product information snippet

            product_data = [
                product_id,
                product_name,
                original_price,
                category,
                ', '.join(image_urls),
                product_url,
                sale_price,
                is_sold_out
            ]
            parsed_data.append(product_data)

        return parsed_data
    def extract_product_id(self, product_url):
        html_content=self.open_link(product_url)
        pid_text='Style:'
        soup_pid=BeautifulSoup(html_content, 'html.parser') if html_content else ''
        if soup_pid:
            style_id_item = soup_pid.find('li', string=lambda text: pid_text in text if text else False)

            if style_id_item:
                # Extract the Style ID
                style_id = style_id_item.text.split(pid_text)[1].strip()
                print(f"Product ID: {style_id}")
                return style_id
            else:
                main_item = soup_pid.find('div',class_='product-more__content')
                style_id_item = main_item.find_all('li')
                if style_id_item:
                    style_id = style_id_item[-1].text.strip()
                    if len(style_id.split(pid_text))>1:
                        style_id=style_id.split(pid_text)[1].strip()
                        return style_id
                    print(f"Product ID: {style_id}")
                    return style_id

                print("Product ID not found.")
        else:
            print(f'Your URL is broken: {product_url}')
class ManoloBlahnikProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'manolo_blahnik')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'category', 'product_name', 'product_id', 'product_url', 'image_urls', 'price', 'discount_price',
            'description'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('div', class_='item product product-item flex flex-col w-full')

        for product in product_blocks:
            # Extract product name
            name_element = product.find('h2', class_='product-info-text')
            product_name = name_element.get_text(strip=True) if name_element else ''

            # Extract product URL and ID from the 'a' tag
            url_element = product.find('a', class_='product-item-photo')
            product_url = url_element.get('href') if url_element else ''

            # Extract product ID from the onclick attribute
            onclick_text = url_element.get('onclick', '') if url_element else ''
            product_id_match = re.search(r'"id":"([^"]+)"', onclick_text)
            product_id = product_id_match.group(1) if product_id_match else ''

            # Extract image URLs
            image_urls = []
            images = product.find_all('img', class_='object-contain w-full')
            for img in images:
                image_urls.append(img.get('src'))

            # Extract price
            full_price_element = product.find('span', class_='price-regular_price')
            if not full_price_element:
                price_element = product.find('span', class_='price-final_price')
                price = price_element.get_text(strip=True) if price_element else ''
                discount_price = ''
            else:
                discount_price_element = product.find('span', class_='price-final_price')
                discount_price = discount_price_element.get_text(strip=True) if discount_price_element else ''
                price_element = product.find('span', class_='price-regular_price')
                price = price_element.get_text(strip=True) if price_element else ''

            # Extract description
            description_element = product.find('div', class_='product-info-short')
            description = description_element.get_text(strip=True) if description_element else ''

            product_data = [
                category,
                product_name,
                product_id,
                product_url,
                ', '.join(image_urls),
                price,
                discount_price,
                description,
            ]
            parsed_data.append(product_data)

        return parsed_data
class GianvitoRossiProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'gianvito_ross')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'category', 'product_name', 'product_id', 'product_url', 'image_urls', 'price','discount_price'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('section', class_='b-product_tile')

        for product in product_blocks:
            # Extract product name
            product_name = product.get('data-product-name', '')

            # Extract product ID
            product_id = product.get('data-product-id', '')

            # Extract product URL
            url_element = product.find('a', class_='b-product_tile-image_link')
            product_url = url_element.get('href') if url_element else ''

            # Extract image URLs
            image_urls = []
            picture_elements = product.find_all('picture', class_='b-product_tile-image')
            for picture in picture_elements:
                img_tag = picture.find('img')
                if img_tag:
                    image_urls.append(img_tag.get('src', ''))

            category_element=product.find('a', class_='b-product_tile-link')
            category_dict=category_element.get('data-analytics','')
            category_dict=json.loads(category_dict) if category_dict else category
            category_internal=category_dict.get('category','')
            if not category_internal:
              category_internal=category
            # Extract price
            price_element = product.find('span', class_='b-price-item')
            price = price_element.get_text(strip=True) if price_element else ''

            discount_price_element = product.find('span', class_='b-price-item m-new')
            discount_price = discount_price_element.get_text(strip=True) if discount_price_element else ''
            if discount_price:
              price_element = product.find('span', class_='b-price-item m-old')
              price = price_element.get_text(strip=True) if price_element else ''
            product_data = [
                category_internal,
                product_name,
                product_id,
                product_url,
                ', '.join(image_urls),
                price,
                discount_price
            ]
            parsed_data.append(product_data)

        return parsed_data
class MiuMiuProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'miu_miu')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'category', 'product_name', 'product_id', 'product_url', 'image_urls', 'price'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('li', class_='h-full')

        for product in product_blocks:
            # Extract product name
            name_element = product.find('h3', class_='card__title')
            product_name = name_element.get_text(strip=True) if name_element else ''

            # Extract product URL
            url_element = product.find('a', class_='h-full')
            product_url = url_element.get('href') if url_element else ''

            # Extract product ID from URL
            product_id = product_url.split('/')[-1] if product_url else ''

            # Extract image URLs
            image_urls = []
            images = product.find_all('source')
            for img in images:
                image_urls.append(img.get('data-srcset', ''))

            # Extract price
            price_element = product.find('p', class_='card__price')
            price = price_element.get_text(strip=True) if price_element else ''

            product_data = [
                category,
                product_name,
                product_id,
                f"https://www.miumiu.com{product_url}" if product_url else '',
                ', '.join(image_urls),
                price
            ]
            parsed_data.append(product_data)

        return parsed_data
class BirkenstockProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'birkenstock')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'category', 'product_name', 'product_id', 'product_url', 'image_urls', 'full_price', 'discount_price'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('li', class_='xlt-producttile')

        for product in product_blocks:
            # Extract product name
            name_element = product.find('div', class_='product-modelname')
            product_name = name_element.get_text(strip=True) if name_element else ''
            product_name += ' ' + product.find('div', class_='product-shortname').get_text(strip=True) if product.find(
                'div', class_='product-shortname') else ''

            # Extract product URL
            url_element = product.find('a', class_='product-tile')
            product_url = url_element.get('href') if url_element else ''

            # Extract image URLs and Product ID
            product_id=None
            image_urls = []
            images = product.find_all('img', class_='standard-tileimage')
            for img in images:
                image_urls.append(img.get('src', ''))
                if not product_id:
                    product_id = ", ".join(
                        img.get('src', '').split("/")[-1].split(".jpg")[0].replace("_sole", '').split("_"))

            # Extract prices
            full_price = ''
            discount_price = ''

            price_element = product.find('span', class_='price-standard')
            if price_element:
                full_price = price_element.get_text(strip=True)

            discount_price_element = product.find('span', class_='price-promotion')
            if discount_price_element:
                discount_price = discount_price_element.get_text(strip=True)
                full_price_element = product.find('span', class_='price-strike')
                if full_price_element:
                    full_price = full_price_element.get_text(strip=True)

            product_data = [
                category,
                product_name,
                product_id,
                f"https://www.birkenstock.com{product_url}" if product_url else '',
                ', '.join(image_urls),
                full_price,
                discount_price
            ]
            parsed_data.append(product_data)

        return parsed_data
class AquazzuraProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'aquazzura')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'category', 'product_name', 'product_id', 'product_url', 'image_url', 'full_price', 'discount_price'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('div', class_='wrapp-single-item')

        for product in product_blocks:
            # Extract product name
            name_element = product.find('h2', class_='card-product__info__intro__link__name')
            product_name = name_element.get_text(strip=True) if name_element else ''

            # Extract product ID
            product_id = product.find('article').get('data-sku', '')

            # Extract product URL
            url_element = product.find('a', class_='card-product__media_link')
            product_url = url_element.get('href') if url_element else ''

            # Extract image URL
            image_element = product.find('img')
            image_url = image_element.get('data-src', '') if image_element else ''

            # Extract prices
            full_price = ''
            discount_price = ''

            price_container = product.find('div', class_='product-price')
            if price_container:
                original_price_element = price_container.find('span', class_='product-price_original')
                if original_price_element:
                    full_price = original_price_element.get_text(strip=True)
                    discount_price_element = price_container.find('div')
                    if discount_price_element:
                        discount_price = discount_price_element.get_text(strip=True).split()[-1]
                else:
                    full_price_element = price_container.find('span')
                    full_price = full_price_element.get_text(strip=True) if full_price_element else ''

            product_data = [
                category,
                product_name,
                product_id,
                f"https://www.aquazzura.com{product_url}" if product_url else '',
                image_url,
                full_price,
                discount_price
            ]
            parsed_data.append(product_data)

        return parsed_data
class HernoProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'herno')
    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'category', 'product_name', 'product_id', 'price', 'discount_price',
            'product_url', 'image_urls', 'color_name', 'currency'
        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('div', class_='b-product_tile')

        for product in product_blocks:
            category = product.get('data-category', '')
            # Extract product name
            name_element = product.find('a', class_='b-product_name')
            product_name = name_element.get_text(strip=True) if name_element else ''

            # Extract product ID and color ID
            product_id = product.get('data-itemid', '')

            # Extract product URL
            product_url = name_element.get('href', '') if name_element else ''

            # Extract image URLs
            image_element = product.find('img', class_='b-product_image')
            image_urls = []
            if image_element:
                main_image = image_element.get('data-src', '')
                alt_image = image_element.get('data-altimage', '')
                main_image_2 = f"https://us.herno.com{main_image}" if main_image else ""
                alt_image_2 = f"https://us.herno.com{alt_image}" if alt_image else ""
                image_urls = list(filter(None, [main_image_2, alt_image_2]))

            # Extract prices
            price_container = product.find('div', class_='b-product_price')
            if price_container:
                price_element = price_container.find('div', class_='b-product_price-standard')
                price = price_element.get_text(strip=True) if price_element else ''

                discount_price_element = price_container.find('span', class_='b-product_price-sales')
                discount_price = discount_price_element.get_text(
                    strip=True) if discount_price_element and not 'h-hidden' in discount_price_element.get('class',
                                                                                                           []) else ''


            else:
                price = ''
                discount_price = ''
                discount_percentage = ''

            # Extract currency
            if '$' in price:
                currency = 'USD'  # Assuming USD based on the $ sign in prices
            else:
                currency = ''
            # Extract color name
            color_name = product.get('data-variant', '')

            # These fields are not directly available in the given HTML

            # Clean up prices
            price = re.sub(r'[^\d.]', '', price)
            discount_price = re.sub(r'[^\d.]', '', discount_price)

            product_data = [
                category,
                product_name,
                product_id,
                price,
                discount_price,
                f"https://us.herno.com{product_url}" if product_url else '',
                '|'.join(image_urls),  # Join multiple image URLs with a separator
                color_name,
                currency
            ]
            parsed_data.append(product_data)

        return parsed_data
class LanvinProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'lanvin')

    def parse_product_blocks(self, soup, category):
        parsed_data = []

        column_names = [
            'category', 'product_id', 'price',
            'product_url', 'image_urls', 'name', 'currency'

        ]
        parsed_data.append(column_names)

        product_blocks = soup.find_all('div', class_='product-item')
        for product in product_blocks:
            # Extract product name
            name_element = product.find('p', class_='sr-product-title')
            name = name_element.find('a').get_text(strip=True) if name_element else ''

            # Extract product URL
            product_url = name_element.find('a')['href'] if name_element else ''
            product_url=f"https://us.lanvin.com{product_url}" if product_url else ''
            product_id=self.extract_product_id(product_url)

            # Extract price
            price_element = product.find('p', class_='product-price')
            price = price_element.get_text(strip=True) if price_element else ''

            # Extract currency
            currency = 'USD' if '$' in price else ''

            # Clean up price
            price = re.sub(r'[^\d.]', '', price)

            # Extract image URLs
            image_elements = product.find_all('div', class_='image')
            image_urls = []
            for img in image_elements:
                img_src = img.find('img')
                if img_src and 'data-srcset' in img_src.attrs:
                    srcset = img_src['data-srcset']
                    highest_res_img = srcset.split(',')[-1].split()[0]
                    image_urls.append(highest_res_img)

            product_data = [
                category,
                product_id,
                price,
                product_url,
                '|'.join(image_urls),
                name,
                currency
            ]
            parsed_data.append(product_data)

        return parsed_data
    def extract_product_id(self, product_url):
        html_content=self.open_link(product_url)
        soup_pid=BeautifulSoup(html_content, 'html.parser') if html_content else ''
        if soup_pid:
            style_id_item = soup_pid.find('span',class_='bodyBase')
            style_id = style_id_item.text.strip() if style_id_item else ''
            if style_id:
                print(f"Found Product ID: {style_id}")
                return style_id

            print("Product ID not found.")
            return style_id
        else:
            print(f'Your URL is broken: {product_url}')
            return ''


class BallyProductParser(WebsiteParser):
    def __init__(self, job_id):
        super().__init__(job_id, 'bally')

    def parse_product_blocks(self, soup,category):
        parsed_data = []

        column_names = [
            'category','brand', 'product_id', 'price',
            'product_url', 'image_urls', 'name', 'currency'
        ]
        parsed_data.append(column_names)
        product_blocks = soup.find_all('div', class_='relative')

        for product in product_blocks:
            # Extract product name
            name_element = product.find('h2')
            name = name_element.find('a').get_text(strip=True) if name_element else ''

            # Extract product URL
            product_url = name_element.find('a')['href'] if name_element else ''

            # Extract product ID
            product_id = self.extract_product_id(product_url)

            # Extract price
            price_element = product.find('span', class_='text-inherit')
            price = price_element.get_text(strip=True) if price_element else ''

            # Extract currency
            if '$' in price:
                currency = 'USD'
            elif '€' in price:
                currency='EURO'
            else:
                currency=''

            # Clean up price
            price = re.sub(r'[^\d.]', '', price)

            # Extract image URLs
            image_element = product.find('img', alt=name)
            image_urls = [image_element['src']] if image_element else []
            if not product_id:
                continue
            product_data = [
                category,
                self.brand,
                product_id,
                price,
                product_url,
                '|'.join(image_urls),
                name,
                currency
            ]
            parsed_data.append(product_data)

        return parsed_data

    def extract_product_id(self, product_url):
        product_id = ''
        if product_url:
            product_id = product_url.split('-')[-1].replace('/','')
        return product_id



def run_parser(job_id, brand_id, source_url):
    print(job_id, brand_id, source_url)
    if brand_id == '93':
        BottegaParser = BottegaVenetaParser(job_id)
        BottegaParser.parse_website(source_url)
    if brand_id == '201':
        FendiParser = FendiProductParser(job_id)
        FendiParser.parse_website(source_url)
    if brand_id == '498':
        StellaParser = StellaProductParser(job_id)
        StellaParser.parse_website(source_url)
    if brand_id == '227':
        GivenchyParser = GivenchyProductParser(job_id)
        GivenchyParser.parse_website(source_url)
    if brand_id == '110':
        CanadaGooseParser = CanadaGooseProductParser(job_id)
        CanadaGooseParser.parse_website(source_url)
    if brand_id == '542':
        VejaParser = VejaProductParser(job_id)
        VejaParser.parse_website(source_url)
    if brand_id == '252':
        IsabelMarantParser = IsabelMarantProductParser(job_id)
        IsabelMarantParser.parse_website(source_url)
    if brand_id == '125':
        ChloeParser = ChloeProductParser(job_id)
        ChloeParser.parse_website(source_url)
    if brand_id == '343':
        MCMParser = MCMProductParser(job_id)
        MCMParser.parse_website(source_url)
    if brand_id == '228':
        GoldenGooseParser = GoldenGooseProductParser(job_id)
        GoldenGooseParser.parse_website(source_url)
    if brand_id == '66':
        BalenciagaParser = BalenciagaProductParser(job_id)
        BalenciagaParser.parse_website(source_url)
    if brand_id == '500':
        StoneIslandParser = StoneIslandProductParser(job_id)
        StoneIslandParser.parse_website(source_url)
    if brand_id == '187':
        EtroParser = EtroProductParser(job_id)
        EtroParser.parse_website(source_url)
    if brand_id == '68':
        BalmainParser = BalmainProductParser(job_id)
        BalmainParser.parse_website(source_url)
    if brand_id == '544':
        VersaceParser = VersaceProductParser(job_id)
        VersaceParser.parse_website(source_url)
    if brand_id == '481':
        FerragamoParser = FerragamoProductParser(job_id)
        FerragamoParser.parse_website(source_url)
    if brand_id == '101':
        BurberryParser = BurberryProductParser(job_id)
        BurberryParser.parse_website(source_url)
    if brand_id == '275':
        KenzoParser = KenzoProductParser(job_id)
        KenzoParser.parse_website(source_url)
    if brand_id == '266':
        JimmyChooParser = JimmyChooProductParser(job_id)
        JimmyChooParser.parse_website(source_url)
    if brand_id == '601':
        BrunelloCucinelliParser = BrunelloCucinelliProductParser(job_id)
        BrunelloCucinelliParser.parse_website(source_url)
    if brand_id == '165':
        DSquaredParser = DSquaredProductParser(job_id)
        DSquaredParser.parse_website(source_url)
    if brand_id == '118':
        CelineParser = CelineProductParser(job_id)
        CelineParser.parse_website(source_url)
    if brand_id == '336':
        MarniParser = MarniProductParser(job_id)
        MarniParser.parse_website(source_url)
    if brand_id == '439':
        PradaParser = PradaProductParser(job_id)
        PradaParser.parse_website(source_url)
    if brand_id == '536':
        ValentinoParser = ValentinoProductParser(job_id)
        ValentinoParser.parse_website(source_url)
    if brand_id == '263':
        JacquemusParser = JacquemusProductParser(job_id)
        JacquemusParser.parse_website(source_url)
    if brand_id == '7':
        AcneStudiosParser = AcneStudiosProductParser(job_id)
        AcneStudiosParser.parse_website(source_url)
    if brand_id == '512':
        TheRowParser = TheRowProductParser(job_id)
        TheRowParser.parse_website(source_url)
    if brand_id == '327':
        ManoloBlahnikParser = ManoloBlahnikProductParser(job_id)
        ManoloBlahnikParser.parse_website(source_url)
    if brand_id == '223':
        GianvitoRossiParser = GianvitoRossiProductParser(job_id)
        GianvitoRossiParser.parse_website(source_url)
    if brand_id == '358':
        miuMiuParser = MiuMiuProductParser(job_id)
        miuMiuParser.parse_website(source_url)
    if brand_id == '46':
        AquazzuraParser = AquazzuraProductParser(job_id)
        AquazzuraParser.parse_website(source_url)
    if brand_id == '523':
        TomFordParser = TomFordProductParser(job_id)
        TomFordParser.parse_website(source_url)
    if brand_id == '604':
        HernoParser = HernoProductParser(job_id)
        HernoParser.parse_website(source_url)
    if brand_id == '286':
        LanvinParser = LanvinProductParser(job_id)
        LanvinParser.parse_website(source_url)
    if brand_id == '67':
        BallyParser = BallyProductParser(job_id)
        BallyParser.parse_website(source_url)
    if brand_id == '317':
        LouboutinParser = LouboutinProductParser(job_id)
        LouboutinParser.parse_website(source_url)
    if brand_id == '374':
        MooseKnucklesParser = MooseKnucklesProductParser(job_id)
        MooseKnucklesParser.parse_website(source_url)
    if brand_id == '???':
        PalmAngelsParser = PalmAngelsProductParser(job_id)
        PalmAngelsParser.parse_website(source_url)
    if brand_id == '???':
        BirkenstockParser = BirkenstockProductParser(job_id)
        BirkenstockParser.parse_website(source_url)
    if brand_id == '???':
        OffWhiteParser = OffWhiteProductParser(job_id)
        OffWhiteParser.parse_website(source_url)
    if brand_id == '???':
        CultGaiaParser = CultGaiaProductParser(job_id)
        CultGaiaParser.parse_website(source_url)


@app.post("/run_parser")
async def brand_batch_endpoint(job_id:str, brand_id: str, scan_url:str,send_out_endpoint_local:str, background_tasks: BackgroundTasks):
    global send_out_endpoint
    send_out_endpoint=send_out_endpoint_local
    background_tasks.add_task(run_parser,job_id, brand_id, scan_url)

    return {"message": "Notification sent in the background"}
if __name__ == "__main__":
    uvicorn.run("main:app", port=8080, host="0.0.0.0", log_level="info")