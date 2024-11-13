import concurrent
import threading

import aiohttp
import asyncio
import oracledb
import pandas as pd
import requests
import time

from datetime import datetime
from duckdb.experimental.spark import DataFrame
from typing import AnyStr, Any, Union

from pandas.core.interchange.dataframe_protocol import Column

from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *
from requests.auth import HTTPBasicAuth

# Column's name where name is a SQL operation
RENAME_COLUMNS = ['number', 'order']


class BronzeSourceBuilderRestAPI(BronzeSourceBuilder):
    """BronzeSourceBuilderRestAPI class"""

    def __init__(self, pSourceProperties: SourceProperties, pBronze_config: BronzeConfig,
                 pBronzeDb_Manager: BronzeDbManager, pLogger: BronzeLogger):
        """BronzeSourceBuilderRestAPI constructor"""

        # Set source properties to REST_API
        vSourceProperties = pSourceProperties._replace(type="REST_API")

        # Call parent constructor for inheritance
        super().__init__(vSourceProperties, pBronze_config, pBronzeDb_Manager, pLogger)

        self.source_database_param = \
            get_parser_config_settings("rest_api")(self.bronze_config.get_configuration_file(),
                                                   self.get_bronze_source_properties().name)

        # Database URL
        self.url = self.source_database_param.url

        # Database user & password
        self.user = self.source_database_param.user
        self.password = self.source_database_param.password

        # Authentication and session
        self.session = requests.Session()
        self.session_auth = HTTPBasicAuth(self.user, self.password)
        self.session.headers.update(({'Accept-Encoding': 'gzip, deflate'}))

        self.columns_change_date_format = []
        self.endpoint = self.bronze_source_properties.table
        self.headers = self.source_database_param.headers
        self.params = self.source_database_param.params
        self.columns_change_date_format = []
        self.LINKS_CACHE = {}
        self.COLUMNS_TYPE_DICT = []

        if self.bronze_source_properties.incremental:
            self.params[
                "sysparm_query"] = f"{self.bronze_source_properties.date_criteria}>{self.bronze_source_properties.last_update}"

    def get_bronze_row_lastupdate_date(self) -> Any:
        """Get Bronze row lasupdate date method"""

        if not self.bronze_date_lastupdated_row:
            v_dict_join = self.get_externaltablepartition_properties()._asdict()
            v_join = " AND ".join(
                [f"{INVERTED_EXTERNAL_TABLE_PARTITION_SYNONYMS.get(key, key)} = '{value}'" for key, value in
                 v_dict_join.items()])

            self.bronze_date_lastupdated_row = \
                self.get_bronzedb_manager().get_bronze_lastupdated_row(self.bronze_table, self.bronze_source_properties.date_criteria, v_join)

        return self.bronze_date_lastupdated_row

    def __set_bronze_bucket_proxy__(self) -> None:
        """Set Bronze bucket proxy method"""
        self.bronze_bucket_proxy.set_bucket_by_extension(p_bucket_extension=self.get_bronze_source_properties().name)

    def __set_bronze_table_settings__(self) -> None:
        """Set Bronze bucket proxy method"""

        v_bronze_table_name = self.get_bronze_source_properties().bronze_table_name

        if not v_bronze_table_name:
            v_bronze_table_name = self.bronze_table = self.get_bronze_source_properties().name \
            + "_" + self.get_bronze_source_properties().schema + "_" + self.get_bronze_source_properties().table.replace(" ", "_")

        self.bronze_table = v_bronze_table_name.upper()
        self.parquet_file_name_template = self.bronze_table
        self.parquet_file_id = 0

        v_dict_externaltablepartition = self.get_externaltablepartition_properties()._asdict()

        self.bucket_file_path = self.get_bronze_source_properties().schema + "/" + '/'.join(
            [f"{key}" for key in v_dict_externaltablepartition.values()]) + "/"
        self.parquet_file_id = self.__get_last_parquet_idx_in_bucket__()

    def get_final_segment(self, link):
        """Get the final segment of the URL"""

        return link.rstrip('/').split('/')[-1]

    def get_columns_type_dict(self, df: DataFrame) -> None:
        """Get columns type dict method"""

        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                self.COLUMNS_TYPE_DICT.append(col)

    def get_columns_to_transform(self, df : DataFrame) -> None:
        """Get columns from df where date format is str(yyyy-MM-DD HH:MM:SS)"""

        for col in df.columns:
            first_non_null_value = next((value for value in df[col] if pd.notnull(value)), None)

            if isinstance(first_non_null_value, str) and len(first_non_null_value) == 19:
                try:
                    # Test conversion if it's possible
                    datetime.strptime(first_non_null_value, "%Y-%m-%d %H:%M:%S")
                    self.columns_change_date_format.append(col)

                except ValueError:
                    pass

    def transform_columns(self, df : DataFrame) -> DataFrame:
        """Change date format, timezone, and columns where column's name is a SQL operation"""

        self.get_columns_to_transform(df)

        for col in self.columns_change_date_format:
            df[col] = pd.to_datetime(df[col])
            df[col] = df[col].dt.tz_localize('UTC')
            df[col] = df[col].dt.tz_convert('Europe/Paris')

        for col in df.columns:
            if col in RENAME_COLUMNS:
                df.rename(columns={col: f"{col}_id"}, inplace=True)

        return df

    def fetch_data(self):
        """Return a DataFrame with data"""

        chunk_size = self.params.get("sysparm_limit", 1000)
        self.params["sysparm_offset"] = self.params.get("sysparm_offset", 0)
        df_list = []

        try:
            while True:
                # Send the HTTP request
                response = requests.get(self.url + self.endpoint, auth=self.session_auth, params=self.params)

                # Check if the request was successful
                if response.status_code != 200:
                    raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

                # Retrieve the JSON data
                data = response.json().get('result', [])

                # If the chunk is empty, break the loop
                if not data:
                    break

                # Convert the data to a DataFrame and append to the list
                chunk_df = pd.DataFrame(data)
                df_list.append(chunk_df)

                # Increase the parameters for pagination
                self.params["sysparm_offset"] += chunk_size
                print(self.params["sysparm_offset"])

            # Check if df_list is not empty before concatenating
            if df_list:
                df = pd.concat(df_list, ignore_index=True)
            else:
                df = pd.DataFrame()

            return df

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

    def update_link_to_value(self, df: pd.DataFrame):
        """Update link to value method using apply"""

        def fetch_and_cache(value):
            if isinstance(value, dict) and 'link' in value:
                link = str(value['link'])
                if link in self.LINKS_CACHE:
                    #print(f"Cache hit for link: {link} -> {self.LINKS_CACHE[link]}")
                    return self.LINKS_CACHE[link]
                else:
                    self.LINKS_CACHE[link] = self.get_value_from_link(link, value)
                    #print(f"Cache miss for link: {link}. Fetching from server... -> {self.LINKS_CACHE[link]}")
                    return self.LINKS_CACHE[link]
            return value

        newDF = df.copy()  # Create a copy of the original DataFrame
        fetch_tasks = []

        # Preliminary loop to collect tasks for fetching values
        for col in self.COLUMNS_TYPE_DICT:
            for value in df[col]:
                if value and isinstance(value, dict) and 'link' in value and value['link'] not in self.LINKS_CACHE:
                    fetch_tasks.append((value['link'],))

        # Use ThreadPoolExecutor to parallelize get_value_from_link calls
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(fetch_tasks)) as executor:
            future_to_link = {executor.submit(fetch_and_cache, {'link': link}): link for link, in fetch_tasks}

            for future in concurrent.futures.as_completed(future_to_link):
                link = future_to_link[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Error fetching link {link}: {e}")

        # Apply the function directly within the loop
        for col in self.COLUMNS_TYPE_DICT:
            start_time = time.time()
            newDF[col] = df[col].apply(fetch_and_cache)
            end_time = time.time()
            print(f"\nIteration {col} in {end_time - start_time} seconds\n")

        return newDF

    def get_value_from_link(self, link, value):
        """Get value from the link"""

        def manage_http_errors(response):
            """Treatment of different http errors possible"""

            # Check if the status code of response is 404 (url not found)
            if response.status_code == 404:
                if self.get_final_segment(link) == 'global':
                    return 'global'
                else:
                    response_content = response.content.decode('utf-8')

                    # Check if the status of response's content is "failure"
                    if response_content == '{"error":{"message":"No Record found","detail":"Record doesn\'t exist or ACL restricts the record retrieval"},"status":"failure"}':
                        return None

                    # Need to check the link to manage this case as http error
                    return 'Need check'

            # Check if the status code of response is 429 (too many requests)
            elif response.status_code == 429:
                return 'Retry'

            elif response.status_code != 200:
                raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

            return response.json().get('result', {}).get('name')

        while True:
            response = requests.get(link, auth=self.session_auth)
            result = manage_http_errors(response)
            if result != 'Retry':
                self.LINKS_CACHE[link] = result
                #print(result)
                return result

    def fetch_source(self, verbose=None) -> bool:
        """Create parquet file(s) from the source"""

        try:
            if verbose:
                message = "Mode {2} : Extracting data {0},{1}".format(self.get_bronze_source_properties().schema,
                                                                          self.get_bronze_source_properties().table,
                                                                          SQL_READMODE)

                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message,
                            log_request=self.request + ': ' + str(self.db_execute_bind))

            self.df_table_content = pd.DataFrame()
            data = None

            # TODO: manage different case between SERVICENOW and CPQ

            # Manage every type of source properties
            match self.get_bronze_source_properties().name:

                # Manage Service_Now source
                case "SERVICE_NOW":

                    data = self.fetch_data()

                    self.transform_columns(data)

                    # Check if the df data is empty
                    if not data.empty:
                        self.get_columns_type_dict(data)

                    data = self.update_link_to_value(data)

                # Manage CPQ source
                case "CPQ":
                    data = data['items']

            self.df_table_content = pd.DataFrame(data)

            # Res None check
            res = self.__create_parquet_file__(verbose)

            if not res:
                raise Exception("Error: creating parquet file")

            self.__update_fetch_row_stats__()

            elapsed = datetime.now() - self.fetch_start

            # On verbose mode, prepare log messages
            if verbose:
                message = "{0} rows in {1} seconds".format(self.total_imported_rows, elapsed)
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "RUN", log_message=message)

            return True

        # Manage every exception thrown, and print depending on exception type
        except (UnicodeDecodeError, oracledb.Error, Exception) as err:

            # Manage Unicode exceptions
            if isinstance(err, UnicodeDecodeError):
                vError = "Error: Unicode Decode, table {}".format(self.get_bronze_source_properties().table)

            # Manage Oracle exceptions
            elif isinstance(err, oracledb.Error):
                vError = "Error: Fetching table {}".format(self.get_bronze_source_properties().table)

            # Manage other exceptions
            else:
                vError = "Error: Fetching table {}. Exception: {}".format(self.get_bronze_source_properties().table, str(err))

            # On verbose mode, prepare log messages
            if verbose:
                log_message = str(err) if not isinstance(err, oracledb.Error) else 'Oracle DB error: ' + str(err)
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=log_message,
                            log_request=self.request)

            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()

            return False