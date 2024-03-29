from .schema import *

from multiprocessing import Pool, cpu_count
from sqlalchemy import create_engine
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from newspaper import Article
from itertools import chain
from functools import wraps
import pandas as pd
import traceback
import psycopg2
import requests
import tempfile
import zipfile
import shutil
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

        self.config = self.read_config(config)

        self.db_name = self.config["db_name"]
        self.db_user = self.config["db_user"]
        self.db_pass = self.config["db_pass"]
        self.db_host = self.config["db_host"]
        self.db_port = self.config["db_port"]

        self.engine = self.get_engine()

    @staticmethod
    def read_config(config):

        try:
            return config if isinstance(config, dict) else json.load(open(config))

        except ValueError as val_err:
            print(f'Configuration Input "{config}" is Not Valid: {val_err}')
            sys.exit(1)

    @staticmethod
    def get_v2_urls():

        return {"last_update": "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"}

    @staticmethod
    def get_v1_urls():

        return {"events": "http://data.gdeltproject.org/events"}

    @staticmethod
    def text_filter(text):

        return re.sub("[^a-zA-Z0-9 \n]", "", text)

    @staticmethod
    def batch_it(l, n):

        for i in range(0, len(l), n):
            yield l[i : i + n]

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
                title = article.title.replace("'", "")
                site = urlparse(article.source_url).netloc
                summary = "{} . . . ".format(article.summary.replace("'", "")[:500])
                summary = re.sub("<.*?>", "", summary)
                keywords = "; ".join(
                    sorted(
                        [re.sub("[^a-zA-Z0-9 \n]", "", key) for key in article.keywords]
                    )
                )
                meta_keys = "; ".join(
                    sorted(
                        [
                            re.sub("[^a-zA-Z0-9 \n]", "", key)
                            for key in article.meta_keywords
                        ]
                    )
                )

                processed_data.append(
                    [event_article[0], title, site, summary, keywords, meta_keys]
                )

            except:
                processed_data.append([event_article[0], None, None, None, None, None])

        return processed_data

    def get_engine(self):

        return create_engine(
            f"postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    def get_connection(self):

        return psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_pass,
            host=self.db_host,
        )

    def temp_handler(func):
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
                print(f"Removed Temp Directory: {temp_dir}")

        return wrap

    def open_connection(func):
        @wraps(func)
        def wrap(*args, **kwargs):
            with args[0].get_connection() as connection:
                with connection.cursor() as cursor:
                    args = list(args)
                    args.insert(1, cursor)
                    return func(*args, **kwargs)

        return wrap

    @open_connection
    def set_geom_field(self, cursor, table_name):

        cursor.execute(
            f"select addgeometrycolumn('{table_name}', 'geom', 4326, 'POINT', 2)"
        )

    @open_connection
    def pop_geom_field(self, cursor, table_name):

        cursor.execute(
            f"update {table_name} set geom = st_setsrid(st_point(actor1geo_long, actor1geo_lat), 4326)"
        )

    def process_article(self, source_url):

        # Parse GDELT Source
        article = Article(source_url)
        article.download()
        article.parse()
        article.nlp()

        # Unpack Article Properties & Replace Special Characters
        title = article.title.replace("'", "")
        site = urlparse(article.source_url).netloc
        summary = "{} . . . ".format(article.summary.replace("'", "")[:500])
        keywords = ", ".join(
            sorted([self.text_filter(key) for key in article.keywords])
        )
        meta_keys = ", ".join(
            sorted([self.text_filter(key) for key in article.meta_keywords])
        )

        return [title, site, summary, keywords, meta_keys]

    @staticmethod
    def extract_csv(csv_name: str, csv_url: str, temp_dir: str) -> str:

        """Fetches URL and extracts CSV content to local directory

        Returns:
            str: Path to local CSV with data for input URL
        """

        response = requests.get(csv_url, stream=True)

        zip_name = csv_url.split("/")[-1]
        zip_path = os.path.join(temp_dir, zip_name)

        with open(zip_path, "wb") as file:
            file.write(response.content)
        with zipfile.ZipFile(zip_path, "r") as the_zip:
            the_zip.extractall(temp_dir)

        txt_name = zip_name.strip(f"{csv_name}.CSV.zip")
        txt_name += f"_{csv_name}.txt"
        txt_path = os.path.join(temp_dir, txt_name)

        os.rename(zip_path.strip(".zip"), txt_path)

        return txt_path

    def article_enrichment(self, article_list):

        if len(article_list) < 100:
            batches = [article_list]
        else:
            batches = list(
                self.batch_it(article_list, int(len(article_list) / cpu_count() - 1))
            )

        # Create Pool & Run Records
        pool = Pool(processes=cpu_count() - 1)
        data = pool.map(self.batch_process_articles, batches)
        pool.close()
        pool.join()

        return list(chain(*data))

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:

        """Process DataFrame for the export data in GDELT 2.0

        Returns:
            DataFrame: Cleaned version of the export extract
        """

        # Keep First Unique URL
        df.drop_duplicates("SOURCEURL", inplace=True)

        print(f"Processing {len(df)} GDELT Records")

        # Process & Append Article Information
        if self.articles:
            article_data = self.article_enrichment(
                df[["GLOBALEVENTID", "SOURCEURL"]].values.tolist()
            )
            article_df = pd.DataFrame(article_data, columns=article_columns)
            df = df.merge(article_df, on="GLOBALEVENTID")

        else:
            df = pd.concat([df, pd.DataFrame(columns=article_columns)])

        # Ensure Columns Are Lowercase
        df.columns = map(str.lower, df.columns)

        return df

    def process_df_mentions(self, df: pd.DataFrame) -> pd.DataFrame:

        """Process DataFrame for the mentions data in GDELT 2.0

        Returns:
            DataFrame: Cleaned version of the mentions extract
        """

        df.columns = map(str.lower, df.columns)

        return df

    def fetch_last_v2_urls(self) -> dict:

        """Return the 3 URLs for GDELT 2.0 - export, mentions, gkg

        Returns:
            dict: A lookup of 2.0 data type (e.g. mentions) to the URL for the data
        """

        response = requests.get(self.v2_urls.get("last_update"))

        data = {}

        rows = [row for row in response.text.split("\n") if row]

        for row in rows:

            url = row.split(" ")[-1]
            label = url.split(".")[-3]

            data.update({label: url})

        if len(data.keys()) != 3:
            raise ValueError(f"Expecting 3 Keys for GDELT 2.0. Received: {data.keys()}")

        return data

    def fetch_last_v1_url(self):

        response = requests.get(f"{self.v1_urls.get('events')}/index.html")
        the_soup = BeautifulSoup(response.content[:2000], features="lxml")
        last_csv = the_soup.find_all("a")[3]["href"]
        last_url = f"{self.v1_urls.get('events')}/{last_csv}"

        return last_url

    def collect_v1_csv(self, temp_dir):

        last_url = self.fetch_last_v1_url()

        csv_file = self.extract_csv(last_url, temp_dir)

        # CSV File Name Will be Converted to Date & Stored in "Extracted_Date" Column
        csv_name = os.path.basename(csv_file).split(".")[0]

        return csv_file, csv_name

    def collect_v2_data(self, temp_dir: str) -> dict:

        """Fetch latest 15 Minute data for GDELT 2.0

        Returns:
            dict: Keys (e.g. gkg) pointing to extracted 2.0 data
        """

        last_data = self.fetch_last_v2_urls()

        # Replace URL Value with Local Path to Extracted Data
        for label, url in last_data.items():
            last_data[label] = self.extract_csv(label, url, temp_dir)

        return last_data

    def get_v2_df(self, file_path: str) -> pd.DataFrame:

        """Load local extract into a DataFrame and return processed version

        Returns:
            DataFrame: Processed DataFrame for the export data in GDELT 2.0
        """

        try:
            df = pd.read_csv(file_path, sep="\t", names=v2_header, dtype=v2_dtypes)

            return self.process_df(df)

        except Exception as gen_exc:
            print(f"Error Building Export SDF: {gen_exc}")

    def get_v2_mentions_df(self, file_path: str) -> pd.DataFrame:

        """Local local extract into a DataFrame and return processed version

        Returns:
            DataFrame: Processed DataFrame for the mentions data in GDELT 2.0
        """

        try:
            df = pd.read_csv(file_path, sep="\t", names=v2_m_header, dtype=v2_m_dtypes)

            return self.process_df_mentions(df)

        except Exception as gen_exc:
            print(f"Error Building SDF: {gen_exc}")

    def get_v1_df(self, csv_file, csv_name):

        try:
            # Convert csv into a pandas dataframe. See schema.py for columns processed from GDELT 2.0
            df = pd.read_csv(csv_file, sep="\t", names=v1_header, dtype=v1_dtypes)

            return self.process_df(df)

        except Exception as gen_exc:
            print(f"Error Building SDF: {gen_exc}")

    @open_connection
    def insert_run(self, cursor, table_name, seconds):

        cursor.execute(f"insert into {table_name} (runtime) values ({seconds})")

    @open_connection
    def get_keywords(self, cursor, table):

        cursor.execute(f"select keywords from {table}")

        return [
            r.strip()
            for r in list(chain(*[r[0].split(";") for r in cursor.fetchall() if r[0]]))
        ]

    def run_v2(self):

        """Load most recent GDELT 2.0 15 minute update into DB """

        start = time.time()
        temp_dir = tempfile.mkdtemp()

        try:
            data_dict = self.collect_v2_data(temp_dir)

            # Handle Exports Table
            v2_df = self.get_v2_df(data_dict.get("export"))
            v2_df.to_sql("v2_exports", self.engine, index=False, if_exists="replace")
            self.set_geom_field("v2_exports")
            self.pop_geom_field("v2_exports")

            # Handle Mentions Table
            v2_mention_df = self.get_v2_mentions_df(data_dict.get("mentions"))
            v2_mention_df.to_sql("v2_mentions", self.engine, index=False, if_exists="replace")

            # Store Last Run
            lr_df = pd.DataFrame({"runtime": [time.time()]})
            lr_df.to_sql("v2_lastrun", self.engine, index=False, if_exists="replace")     

        finally:
            shutil.rmtree(temp_dir)
            print(f"Ran V2 Solution: {round((time.time() - start) / 60, 2)}")

    def run_v1(self):

        start = time.time()

        temp_dir = tempfile.mkdtemp()
        v1_table = "v1"

        try:
            csv_file, csv_name = self.collect_v1_csv(temp_dir)
            v1_df = self.get_v1_df(csv_file, csv_name)

            v1_df.to_sql(v1_table, self.engine, index=False, if_exists="replace")

            self.set_geom_field(v1_table)
            self.pop_geom_field(v1_table)

            lr_df = pd.DataFrame({"runtime": [time.time()]})
            lr_df.to_sql("v1_lastrun", self.engine, index=False, if_exists="replace")

        finally:
            shutil.rmtree(temp_dir)
            print(f"Ran V1 Solution: {round((time.time() - start) / 60, 2)}")
