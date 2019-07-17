from .v2_table import text_base, geom_base

from newspaper import Article, ArticleException
from multiprocessing import Pool, cpu_count
from functools import wraps, partial
from urllib.parse import urlparse
import traceback
import datetime
import calendar
import psycopg2
import requests
import tempfile
import logging
import zipfile
import shutil
import json
import time
import sys
import csv
import re
import os


class Extractor(object):

    def __init__(self, config):

        # Set Date & Time
        self.date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d')
        self.time = datetime.datetime.fromtimestamp(time.time()).strftime('%H_%M_%S')

        # Load Configuration
        self.config_dir = os.path.dirname(config)
        self.config = self.read_config(config)

        # Create Log
        self.logdir = os.path.join(self.config_dir, 'logs', self.date)
        self.logger = self.get_logger()

        self.db_name = self.config['db_name']
        self.db_user = self.config['db_user']
        self.db_pass = self.config['db_pass']
        self.db_host = self.config['db_host']

        self.v2_urls = self.get_v2_urls()

        self.latest_src = 'gdelt_latest_src'
        self.latest_dst = 'gdelt_latest_dst'

    @staticmethod
    def read_config(config):

        try:
            return config if isinstance(config, dict) else json.load(open(config))

        except ValueError as val_err:
            print(f'Configuration Input "{config}" is Not Valid: {val_err}')
            sys.exit(1)

    def get_logger(self):

        """ Ensures log_dir Directory Exists, Build & Returns Simple Logger """

        the_logger = logging.getLogger('Extractor')
        the_logger.setLevel(logging.DEBUG)

        # Ensure Directories Exist
        if not os.path.exists(self.logdir):
            os.makedirs(self.logdir)

        # Set Console Handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # Set File Handler
        fh = logging.FileHandler(os.path.join(self.logdir, f'Extractor_{self.time}.log'), 'w')
        fh.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        ch.setFormatter(formatter)
        fh.setFormatter(formatter)

        the_logger.addHandler(ch)
        the_logger.addHandler(fh)

        the_logger.info('Logger Initialized')

        return the_logger

    @staticmethod
    def get_v2_urls():

        return {
            'last_update': 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
        }

    @staticmethod
    def get_date_range(y, m):

        return [
            datetime.date(y, m, day).strftime('%Y%m%d') for day in range(1, calendar.monthrange(y, m)[1] + 1)
        ]

    @staticmethod
    def extract_daily_csv(target_date):

        # Pull CSV from GDELT Repository
        date_zip = '{}.export.CSV.zip'.format(target_date)
        event_url = 'http://data.gdeltproject.org/events/{}'.format(date_zip)
        response = requests.get(event_url, stream=True)

        if response.status_code != 200:
            return None

        # Dumpt to Local CSV
        temp_dir = tempfile.mkdtemp(dir=r'C:\Temp', prefix='{}_'.format(target_date))
        zip_file = '{}/{}.zip'.format(temp_dir, target_date)
        with open(zip_file, 'wb') as f: f.write(response.content)
        with zipfile.ZipFile(zip_file, 'r') as the_zip: the_zip.extractall(temp_dir)

        return '{}/{}.export.CSV'.format(temp_dir, target_date)

    @staticmethod
    def text_filter(text):

        return re.sub('[^a-zA-Z0-9 \n]', '', text)

    def get_connection(self):

        return psycopg2.connect(dbname=self.db_name, user=self.db_user, password=self.db_pass, host=self.db_host)

    def open_connection(func):

        """ Inserts Cursor Object As First Arguement of Function """

        @wraps(func)
        def wrap(*args, **kwargs):
            with args[0].get_connection() as connection:
                with connection.cursor() as cursor:
                    args = list(args)
                    args.insert(1, cursor)
                    return func(*args, **kwargs)
        return wrap

    def process_article(self, source_url):

        # Parse GDELT Source
        article = Article(source_url)
        article.download()
        article.parse()
        article.nlp()

        # Unpack Article Properties & Replace Special Characters
        title = self.text_filter(article.title)
        summary = '{} . . . '.format(self.text_filter(article.summary)[:500])
        keywords = ', '.join(sorted([self.text_filter(key) for key in article.keywords]))
        meta_keys = ', '.join(sorted([self.text_filter(key) for key in article.meta_keywords]))
        site = urlparse(article.source_url).netloc

        return [title, site, summary, keywords, meta_keys]

    def process_events(self, year, target_csv):

        # Tracking
        seen_urls = []
        proc_urls = 0

        # Extract Records
        with open(target_csv, newline='', encoding='utf8') as the_csv:

            the_reader = csv.reader(the_csv, delimiter='\t')

            for idx, row in enumerate(the_reader, start=1):

                # Pull Filter Attributes
                avg_tone = float(row[34])  # Average Tone
                src_url = row[57]  # Source URL
                a1_geo_lat = row[39]  # Latitude Check
                a1_gc = row[37]  # Actor1Geo_Country
                a2_geo_lat = row[39]  # Longitude Check
                a2_gc = row[44]  # Actor1Geo_Country

                try:
                    # TODO - Actor1Geo_Type in ('2', '3')
                    if all([v == 'US' for v in [a1_gc, a2_gc]]) \
                            and avg_tone < 0 \
                            and src_url not in seen_urls \
                            and all([a1_geo_lat, a2_geo_lat]):

                        # Extract NLP Values with Article
                        derived_attributes = self.process_article(src_url)

                        # Push Values into Master Table
                        with self.get_connection() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    '''
                                    insert into gdelt_{}
                                    values {}
                                    '''.format(year, tuple(row + derived_attributes))
                                )

                        proc_urls += 1

                except ArticleException:
                    pass

                except:
                    print(f'{traceback.format_exc()}')

                finally:
                    seen_urls.append(src_url)

    def process_day(self, year, the_day):

        print(f'Processing Day: {the_day}')

        # Download GDELT Records Locally for Processing
        daily_csv = self.extract_daily_csv(the_day)

        # Ignore Bad CSV Requests
        if not daily_csv: return

        # Collect Enriched Values & Push Into Table
        self.process_events(year, daily_csv)

        # Remove Temporary Directory
        shutil.rmtree(os.path.dirname(daily_csv))

    def run_month(self, month, year):

        date_range = self.get_date_range(year, month)

        # Create Pool & Run Records
        pool = Pool(processes=cpu_count() - 1)
        pool.map(partial(self.process_day, year), date_range)
        pool.close()
        pool.join()

    def extract_csv(self, csv_url):

        response = requests.get(csv_url, stream=True)

        temp_dir = tempfile.mkdtemp(dir=self.logdir)

        zip_name = csv_url.split('/')[-1]
        zip_path = os.path.join(temp_dir, zip_name)

        with open(zip_path, 'wb') as file: file.write(response.content)
        with zipfile.ZipFile(zip_path, 'r') as the_zip: the_zip.extractall(temp_dir)

        txt_name = zip_name.strip('export.CSV.zip')
        txt_name += '.txt'
        txt_path = os.path.join(temp_dir, txt_name)

        os.rename(zip_path.strip('.zip'), txt_path)

        return txt_path, temp_dir

    @open_connection
    def check_table(self, cursor, table_name):

        cursor.execute(f'''
                       select tablename from pg_tables
                       where tablename = '{table_name}'
                       ''')

        res = [row[0] for row in cursor.fetchall()]

        if len(res) == 1:
            return True

        return False

    @open_connection
    def delete_table(self, cursor, table_name):

        self.logger.info('Dropping Table: {}'.format(table_name))

        cursor.execute(f'drop table if exists {table_name}')

    @open_connection
    def create_table(self, cursor, table_name):

        self.logger.info(f'Creating Table: {table_name}')

        cursor.execute(text_base.format(table_name))

    @open_connection
    def load_latest(self, cursor, table_name, text_data):

        self.logger.info(f'Loading Data into Table: {table_name}')

        with open(text_data, 'r', encoding='latin-1') as raw_data:
            cursor.copy_from(raw_data, table_name)

    @open_connection
    def load_subset(self, cursor, src, dst):

        cursor.execute(geom_base.format(dst, src))

    @open_connection
    def set_geom_field(self, cursor, table_name):

        cursor.execute(f"select addgeometrycolumn('{table_name}', 'geom', 4326, 'POINT', 2)")

    @open_connection
    def pop_geom_field(self, cursor, table_name):

        cursor.execute(f"update {table_name} set geom = st_setsrid(st_point(actor1geo_long, actor1geo_lat), 4326)")

    def process_latest(self):

        # Fetch URL Information for Latest CSV
        response = requests.get(self.v2_urls.get('last_update'))
        last_url = [r for r in response.text.split('\n')[0].split(' ') if 'export' in r][0]

        # Pull & Extract Latest CSV
        self.logger.info(f'Processing Export CSV: {last_url}')
        csv_file, tmp_path = self.extract_csv(last_url)

        # Delete Existing Latest Tables
        for table in [self.latest_src, self.latest_dst]:
            if self.check_table(table):
                self.delete_table(table)

        # Create All Text Baseline & Load Latest CSV Data
        self.create_table(self.latest_src)
        self.load_latest(self.latest_src, csv_file)

        # Populate Table with Correct Types & Limited Attributes
        self.load_subset(self.latest_src, self.latest_dst)

        # Populate Table with Geometries
        self.set_geom_field(self.latest_dst)
        self.pop_geom_field(self.latest_dst)

        # Remove Temporary Files
        shutil.rmtree(tmp_path)




