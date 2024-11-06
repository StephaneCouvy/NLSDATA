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

LINKS_CACHE = {}
COLUMNS_TYPE_DICT = []

# Column's name where name is a SQL operation
RENAME_COLUMNS = ['number', 'order']


def set_columns_type_dict(df : DataFrame) -> None:
    """Get columns type dict method"""
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            COLUMNS_TYPE_DICT.append(col)


def get_final_segment(link : str) -> Any | None:
    """Get final segment method"""

    final_segment = link.rsplit('/', 1)[-1]

    if final_segment == 'global':
        LINKS_CACHE[link] = final_segment
        return final_segment

    else:
        LINKS_CACHE[link] = None
        return None


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

        if self.bronze_source_properties.incremental:
            self.params[
                "sysparm_query"] = f"{self.bronze_source_properties.date_criteria}>{self.bronze_source_properties.last_update}"

        self.response = requests.get(self.url + self.endpoint, auth=self.session_auth, params=self.params)
        self.response_data = self.response.json()

        if self.response.status_code != 200:
            vError = "ERROR connecting to : {}".format(self.get_bronze_source_properties().name)
            raise Exception(vError)

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

    def fetch_chunk(self) -> Union[list, pd.DataFrame]:
        """Fetch chunk data method"""

        chunk_size = self.params["sysparm_limit"]
        df = pd.DataFrame()

        try:
            while True:
                # Send HTTP request
                start_time = time.time()
                #response = self.requests.get(self.url + self.endpoint, auth=self.session_auth, params=self.params)

                response = self.session.get(self.url + self.endpoint, auth=self.session_auth, params=self.params)

                print('FETCH CHUNK: ', time.time() - start_time)

                # Check if the request was successful
                if response.status_code != 200:
                    raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

                new_start_time = time.time()

                # Retrieve JSON data
                data = self.response.json()
                result_data = data.get('result', [])
                print('RESULT:', time.time() - new_start_time)

                # If the chunk is empty, break the loop
                if not result_data:
                    break

                new1_start_time = time.time()

                # Convert data to DataFrame and concatenate
                chunk_df = pd.DataFrame(result_data)
                df = pd.concat([df, chunk_df], ignore_index=True)

                print('CONCAT :', time.time() - new1_start_time)

                # Increase parameters for pagination
                self.params["sysparm_offset"] += chunk_size

            return df

        except Exception as e:
            print(f"Error: An unexpected error occurred: {e}")
            return []

    def set_columns_to_transform(self, df : DataFrame) -> None:
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

        self.set_columns_to_transform(df)

        for col in self.columns_change_date_format:
            df[col] = df[col].str.replace('-', '/', regex=False)
            df[col] = pd.to_datetime(df[col], format='%Y/%m/%d %H:%M:%S')
            df[col] = df[col].dt.tz_localize('UTC')
            df[col] = df[col].dt.tz_convert('Europe/Paris')

        for col in df.columns:
            if col in RENAME_COLUMNS:
                df.rename(columns={col: f"{col}_id"}, inplace=True)

        return df

    def fetch_all_data(self) -> Union[DataFrame | list | None]:
        """Return a df with all incidents"""

        all_incidents_df = self.fetch_chunk()

        # Check if 'all_incidents_df' is empty
        if all_incidents_df.empty:
            print("No data fetched.")
            return None

        return all_incidents_df

    def fetch_value_from_link(self, link: str):
        """Fetch value from link method"""

        start_time = time.time()

        try:
            request_start_time = time.time()
            data_link = self.session.get(link, auth=self.session_auth).json()
            request_end_time = time.time()

            get_start_time = time.time()
            result = data_link.get('result', {})
            name = result.get('name')
            number = result.get('number')
            get_end_time = time.time()

            if name:
                print('--------------------------------RESULT BY NAME--------------------------------')
                LINKS_CACHE[link] = name
                print('LINK : ', link, ' -> ', name)
            elif number:
                print('--------------------------------RESULT BY NUMBER--------------------------------')
                LINKS_CACHE[link] = number
                print('LINK : ', link, ' -> ', number)
            else:
                return get_final_segment(link)

            end_time = time.time()

            print('\033[94mGet time : ', get_end_time - get_start_time, ' seconds\033[0m')
            print('\033[94mRequest time : ', request_end_time - request_start_time, ' seconds\033[0m')
            print('\033[94mProcess time : ', end_time - start_time, ' seconds\033[0m')

            return name if name else number

        except Exception as e:
            print(f"Error: HTTP invalid behavior occurred: {e} for URL: {link}")
            return get_final_segment(link)

    def update_link_to_value(self, df: DataFrame) -> DataFrame:
        """Update link to value method using apply"""

        def replace_link(value):
            if value:
                link = value['link']

                if link in LINKS_CACHE:
                    return LINKS_CACHE[link]
                else:
                    return self.fetch_value_from_link(str(link))

            return value

        i = 0

        newDF = df.copy()  # Create a copy of the original DataFrame

        # Browse the registered columns type dictionary
        for col in COLUMNS_TYPE_DICT:
            start_time = time.time()
            newDF[col] = df[col].apply(replace_link)  # Apply the function and store the result in the new DataFrame
            end_time = time.time()
            print('\033[94m\nIteration ', i, ' in ', end_time - start_time, ' seconds\n\033[0m')
            i += 1

        return newDF

    def fetch_source(self, verbose=None) -> bool:
        """Create parquet file(s) from the source"""

        try:
            # Check response status code, on 200 throw an Exception
            if self.response.status_code != 200:
                raise Exception("Error: no DB connection")

            else:
                # On verbose mode, prepare log messages
                if verbose:
                    message = "Mode {2} : Extracting data {0},{1}".format(self.get_bronze_source_properties().schema,
                                                                          self.get_bronze_source_properties().table,
                                                                          SQL_READMODE)

                    verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message,
                                log_request=self.request + ': ' + str(self.db_execute_bind))

                self.df_table_content = pd.DataFrame()
                data = self.response.json()

                # TODO: manage different case between SERVICENOW and CPQ

                # Manage every type of source properties
                match self.get_bronze_source_properties().name:

                    # Manage Service_Now source
                    case "SERVICE_NOW":

                        data = self.fetch_all_data()

                        # Data None check
                        if not data.empty:
                            data = self.transform_columns(data)
                        else:
                            return False

                        # Data None check
                        if not data.empty:
                            set_columns_type_dict(data)
                        else:
                            return False

                        start_time = time.time()
                        data = self.update_link_to_value(data)
                        end_time = time.time()

                        treatment_link_time = end_time - start_time
                        print(treatment_link_time)

                    # Manage CPQ source
                    case "CPQ":
                        data = data['items']

                self.df_table_content = pd.DataFrame(data)

                res = self.__create_parquet_file__(verbose)

                # Res None check
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