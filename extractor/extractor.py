from .schema import v2_header, v1_header, article_columns, cameo, dtype_map

from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from newspaper import Article
from itertools import chain
from functools import wraps
import pandas as pd
import numpy as np
import traceback
import requests
import tempfile
import zipfile
import shutil
import pytz
import json
import time
import sys
import os
import re

import warnings
warnings.filterwarnings("ignore")


class Extractor(object):

    def __init__(self, config):

        self.scratch = os.path.split(os.path.realpath(__file__))[0]

        self.v2_urls = self.get_v2_urls()
        self.v1_urls = self.get_v1_urls()

        self.articles = True

        # Load Configuration
        self.config_dir = os.path.dirname(config)
        self.config = self.read_config(config)

        self.db_name = self.config['db_name']
        self.db_user = self.config['db_user']
        self.db_pass = self.config['db_pass']
        self.db_host = self.config['db_host']

        self.latest_src = 'gdelt_latest_src'
        self.latest_tmp = 'gdelt_latest_tmp'
        self.latest_dst = 'gdelt_latest_dst'
        self.latest_run = 'gdelt_latest_run'

    @staticmethod
    def read_config(config):

        try:
            return config if isinstance(config, dict) else json.load(open(config))

        except ValueError as val_err:
            print(f'Configuration Input "{config}" is Not Valid: {val_err}')
            sys.exit(1)

    @staticmethod
    def get_v2_urls():

        return {
            'last_update': 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
        }

    @staticmethod
    def get_v1_urls():

        return {
            'events': 'http://data.gdeltproject.org/events'
        }

    @staticmethod
    def text_filter(text):

        return re.sub('[^a-zA-Z0-9 \n]', '', text)

    @staticmethod
    def batch_it(l, n):

        for i in range(0, len(l), n):
            yield l[i:i + n]

    @staticmethod
    def batch_process_articles(article_list):

        print(f"Subprocess Handling {len(article_list)} Articles")

        processed_data = []

        for event_article in article_list:

            try:
                # Parse GDELT Source
                article = Article(event_article[1])
                article.download()
                article.parse()
                article.nlp()

                # Unpack Article Properties & Replace Special Characters
                title     = article.title.replace("'", '')
                site      = urlparse(article.source_url).netloc
                summary   = '{} . . . '.format(article.summary.replace("'", '')[:500])
                summary   = re.sub('<.*?>', '', summary)
                keywords  = '; '.join(sorted([re.sub('[^a-zA-Z0-9 \n]', '', key) for key in article.keywords]))
                meta_keys = '; '.join(sorted([re.sub('[^a-zA-Z0-9 \n]', '', key) for key in article.meta_keywords]))

                processed_data.append([event_article[0], title, site, summary, keywords, meta_keys])

            except:
                processed_data.append([event_article[0], None, None, None, None, None])

        return processed_data

    def get_connection(self):

        return psycopg2.connect(dbname=self.db_name, user=self.db_user, password=self.db_pass, host=self.db_host)

    def temp_handler(func):
        """
        Wrapper function that appends a temporary file directory value that's passed into
        the get_v2_sdf function. The directory path is used to temporarily store the .csv
        downloaded for processing. After processing has finished, file contents and directory
        are removed.
        """

        @wraps(func)
        def wrap(*args, **kwargs):

            temp_dir = tempfile.mkdtemp()

            args = list(args)
            args.insert(1, temp_dir)

            try:
                func(*args, **kwargs)
            except:
                print(traceback.format_exc())
            finally:
                shutil.rmtree(temp_dir)
                print(f'Removed Temp Directory: {temp_dir}')

        return wrap

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
        title     = article.title.replace("'", '')
        site      = urlparse(article.source_url).netloc
        summary   = '{} . . . '.format(article.summary.replace("'", '')[:500])
        keywords  = ', '.join(sorted([self.text_filter(key) for key in article.keywords]))
        meta_keys = ', '.join(sorted([self.text_filter(key) for key in article.meta_keywords]))

        return [title, site, summary, keywords, meta_keys]

    @staticmethod
    def extract_csv(csv_url, temp_dir):

        response = requests.get(csv_url, stream=True)

        zip_name = csv_url.split('/')[-1]
        zip_path = os.path.join(temp_dir, zip_name)

        with open(zip_path, 'wb') as file: file.write(response.content)
        with zipfile.ZipFile(zip_path, 'r') as the_zip: the_zip.extractall(temp_dir)

        txt_name = zip_name.strip('export.CSV.zip')
        txt_name += '.txt'
        txt_path = os.path.join(temp_dir, txt_name)

        os.rename(zip_path.strip('.zip'), txt_path)

        return txt_path

    def article_enrichment(self, article_list):

        if len(article_list) < 100:
            batches = [article_list]
        else:
            batches = list(self.batch_it(article_list, int(len(article_list) / cpu_count() - 1)))

        # Create Pool & Run Records
        pool = Pool(processes=cpu_count() - 1)
        data = pool.map(self.batch_process_articles, batches)
        pool.close()
        pool.join()

        return list(chain(*data))

    def process_df(self, df, extracted_date, ext_geo, articles=True):

        print(f'Processing {len(df)} GDELT Records')

        # Discard Anything Without a Coordinate
        df.dropna(subset=['ActionGeo_Long', 'ActionGeo_Lat'], inplace=True)

        # Keep First Unique URL
        df.drop_duplicates('SOURCEURL', inplace=True)

        # Get Most of the Things - We Need to Use our Schema to be Smarter About This
        df.fillna('0', inplace=True)

        # Insert Value to Check on Future Runs
        df['extracted_date'] = pd.to_datetime(extracted_date).replace(tzinfo=pytz.UTC)

        # Avoid Python Integer Overflow Errors
        df['GLOBALEVENTID'] = df['GLOBALEVENTID'].astype('str')
        df['EventRootCode'] = df['EventRootCode'].astype('str')
        df['DATEADDED']     = df['DATEADDED'].astype('str')

        # Map Root Code to Cameo Definition
        df['CATEGORY'] = df['EventRootCode'].apply(lambda x: cameo.get(x, 'Other'))

        # Build Geometry
        df = df.spatial.from_xy(df, 'ActionGeo_Long', 'ActionGeo_Lat')

        # Filter Only Those Intersecting Input Filter
        intersects = df.SHAPE.geom.disjoint(ext_geo) == False
        geom_df = df[intersects]

        # Append Article Information
        if articles:
            article_data = self.article_enrichment(geom_df[['GLOBALEVENTID', 'SOURCEURL']].values.tolist())
            a_df = pd.DataFrame(article_data, columns=article_columns)
            geom_df = df.merge(a_df, on='GLOBALEVENTID')

        return geom_df

    def fetch_last_v2_url(self):
        """
        Grab the V2 export .csv from the latest update URL. The url contains a list of three
        packages that can be downloaded.  This function will return the export package in
        the list.  This represents the newest events in the 15 minute dump.
        """
        response = requests.get(self.v2_urls.get('last_update'))
        last_url = [r for r in response.text.split('\n')[0].split(' ') if 'export' in r][0]

        return last_url

    def fetch_last_v1_url(self):
        """
        Grab the V1 export .csv from the events index URL. The url contains a list of daily
        packages that can be downloaded, dating back to 2013-04-01. This function will return
        the latest package in the list.  This represents the newest events in the 24 hour dump.
        """

        response = requests.get(f"{self.v1_urls.get('events')}/index.html")
        the_soup = BeautifulSoup(response.content[:2000], features='lxml')
        last_csv = the_soup.find_all('a')[3]['href']
        last_url = f"{self.v1_urls.get('events')}/{last_csv}"

        return last_url

    def collect_v1_csv(self, temp_dir):

        """
        Collects Latest V1 CSV & Returns Path to CSV & CSV Name (Extraction Date)
        """

        last_url = self.fetch_last_v1_url()

        csv_file = self.extract_csv(last_url, temp_dir)

        # CSV File Name Will be Converted to Date & Stored in "Extracted_Date" Column
        csv_name = os.path.basename(csv_file).split('.')[0]

        return csv_file, csv_name

    def collect_v2_csv(self, temp_dir):

        """
        Collects Latest V2 CSV & Returns Path to CSV & CSV Name (Extraction Date)
        """

        last_url = self.fetch_last_v2_url()

        csv_file = self.extract_csv(last_url, temp_dir)

        # CSV File Name Will be Converted to Date & Stored in "Extracted_Date" Column
        csv_name = os.path.basename(csv_file).split('.')[0]

        return csv_file, csv_name

    def get_v2_sdf(self, csv_file, csv_name, ext_geo):
        """
        Process GDELT 2.0 event data in a .csv format and convert into a spatial data frame.
        """

        try:
            # Convert csv into a pandas dataframe. See schema.py for columns processed from GDELT 2.0
            df = pd.read_csv(csv_file, sep='\t', names=v2_header, dtype=dtype_map)

            return self.process_df(df, csv_name, ext_geo)

        except Exception as gen_exc:
            print(f'Error Building SDF: {gen_exc}')

    def get_v1_sdf(self, csv_file, csv_name):
        """
        Process GDELT 1.0 event data in a .csv format and convert into a spatial data frame.
        """

        try:
            # Convert csv into a pandas dataframe. See schema.py for columns processed from GDELT 2.0
            df = pd.read_csv(csv_file, sep='\t', names=v1_header, dtype=dtype_map)

            return self.process_df(df, csv_name)

        except Exception as gen_exc:
            print(f'Error Building SDF: {gen_exc}')

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

    @open_connection
    def create_column(self, cursor, table, col_name, col_type):

        cursor.execute(f"alter table {table} add column {col_name} {col_type};")

    @open_connection
    def rename_table(self, cursor, old, new):

        cursor.execute(f"alter table {old} rename to {new}")

    @open_connection
    def create_run_table(self, cursor, table_name):

        cursor.execute(run_base.format(table_name))

    @open_connection
    def insert_run(self, cursor, table_name, seconds):

        cursor.execute(f"insert into {table_name} (runtime) values ({seconds})")

    @open_connection
    def get_keywords(self, cursor):

        cursor.execute(f"select keywords from {self.latest_dst}")

        return [r.strip() for r in list(chain(*[r[0].split(',') for r in cursor.fetchall() if r[0]]))]

    @open_connection
    def remove_duplicates(self, cursor, table):

        cursor.execute(f"select globaleventid, sourceurl from {table}")

        deletions = []
        seen_urls = []
        for row in cursor.fetchall():
            if row[1] not in seen_urls:
                seen_urls.append(row[1])
            else:
                deletions.append(row[0])

        cursor.execute(f"delete from {table} where globaleventid in {tuple(deletions)}")

    def process_latest(self):

        # Process Started
        start = time.time()

        # Fetch URL Information for Latest CSV
        response = requests.get(self.v2_urls.get('last_update'))
        last_url = [r for r in response.text.split('\n')[0].split(' ') if 'export' in r][0]

        # Pull & Extract Latest CSV
        self.logger.info(f'Processing Export CSV: {last_url}')
        csv_file, tmp_path = self.extract_csv(last_url)

        # Delete Existing Latest Tables
        for table in [self.latest_src, self.latest_tmp]:
            if self.check_table(table):
                self.delete_table(table)

        # Create All Text Baseline & Load Latest CSV Data
        self.create_table(self.latest_src)
        self.load_latest(self.latest_src, csv_file)

        # Populate Table with Correct Types & Limited Attributes
        self.load_subset(self.latest_src, self.latest_tmp)

        # Populate Table with Geometries
        self.set_geom_field(self.latest_tmp)
        self.pop_geom_field(self.latest_tmp)

        # Create Columns for Article Processing
        self.create_column(self.latest_tmp, 'meta_keys', 'text')
        self.create_column(self.latest_tmp, 'keywords', 'text')
        self.create_column(self.latest_tmp, 'summary', 'text')
        self.create_column(self.latest_tmp, 'title', 'text')
        self.create_column(self.latest_tmp, 'site', 'text')

        # Remove "Duplicate" Entries
        self.remove_duplicates(self.latest_tmp)

        # Enrich from Articles
        self.process_events(self.latest_tmp)

        # Dump Existing Destination & Replace With New Data
        if self.check_table(self.latest_dst):
            self.delete_table(self.latest_dst)
        self.rename_table(self.latest_tmp, self.latest_dst)

        # Remove Temporary Files
        shutil.rmtree(tmp_path)

        # Ensure Run Table Exists
        if not self.check_table(self.latest_run):
            self.create_run_table(self.latest_run)

        # Push Latest Run
        self.insert_run(self.latest_run, time.time())

        # Run Time
        self.logger.info(f'Ran: {round((time.time() - start) / 60, 2)}')

    @temp_handler
    def run_v2(self, temp_dir, hfl_id, ext_geo):
        """
        Runner function to extract, process and push events from GDELT 2.0 into an existing hosted feature layer and table.

        NOTE: If Hosted feature layer and table do not exist, it's recommend to run the build_V2 function to create layer
        with the necessary schema to load data into.
        """

        start = time.time()

        try:
            # Flag for Summary Table Deletion
            past_date = (datetime.utcnow() - timedelta(hours=self.max_age))

            # Process Latest 15 Minute Hosted Feature Layer
            all_itm = self.get_gis_item(hfl_id, self.gis)
            all_lyr = all_itm.layers[0]
            all_sdf = all_lyr.query(out_fields='extracted_date', return_geometry=False).sdf

            # Collect & Unpack Latest 15 Minute CSV Dump
            csv_file, csv_name = self.collect_v2_csv(temp_dir)
            csv_date = pd.to_datetime(csv_name).replace(tzinfo=pytz.UTC)

            # Skip Anything Already Processed
            if len(all_sdf) > 0 and np.datetime64(csv_date) in all_sdf['extracted_date'].unique():
                print(f'Data Already Extracted for Current Date: {csv_date}')
                return

            # Convert Current 15 Minute GDELT Data to Spatial Data Frame
            new_df = self.get_v2_sdf(csv_file, csv_name, ext_geo)

            # Remove Data Older Than Max Age from GDELT 2.0 hosted feature layer table.
            # Return If Date Already Processed
            if len(all_sdf):
                self.delete(all_lyr, all_sdf, 'extracted_date', all_lyr.properties.objectIdField, past_date)

            # Push New Data
            self.process_edits(all_lyr, new_df, 'add')

        finally:
            print(f'Ran V2 Solution: {round((time.time() - start) / 60, 2)}')

    @temp_handler
    def run_v1(self, temp_dir, hft_id, gdb_path):
        """
        Runner function to extract, process and push events from GDELT 1.0 into an existing hosted feature layer and table.

        NOTE: If Hosted feature layer and table do not exist, it's recommend to run the build_v1 function to create layer
        with the necessary schema to load data into.
        """

        # Process Started
        start = time.time()

        try:
            # Collect & Unpack Latest Daily CSV Dump
            csv_file, csv_name = self.collect_v1_csv(temp_dir)

            # Get Data Frame with SHAPE Attributes
            df = self.get_v1_sdf(csv_file, csv_name)

            # Create Local Feature Class
            fc = df.spatial.to_featureclass(os.path.join(gdb_path, f'V1_{csv_name}'), overwrite=True)
            print(f"Created Local Feature Class: {fc}")

        finally:
            print(f'Ran V1 Solution: {round((time.time() - start) / 60, 2)}')
