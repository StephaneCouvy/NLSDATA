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

CHANGE_DATE_FORMAT = ['sys_updated_on', 'inc_sys_updated_on', 'md_sys_updated_on', 'mi_sys_updated_on',
                      'sys_created_on', 'closed_at', 'opened_at', 'business_duration', 'activity_due', 'sla_due'
                      'calendar_duration', 'requested_by_date', 'approval_set', 'end_date', 'work_start', 'start_date',
                      'work_end', 'conflict_last_run', 'resolved_at', 'u_duration_calc', 'reopened_time', 'u_state_changed_date',
                      'inc_u_duration_calc', 'mi_sys_created_on', 'inc_sys_created_on', 'inc_business_duration', 'due_date'
                      'inc_calendar_duration', 'md_sys_created_on', 'inc_opened_at', 'inc_resolved_at', 'inc_closed_at', 'mi_business_duration', 'mi_duration', 'mi_start', 'mi_end']

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

        # Database user
        self.user = self.source_database_param.user

        # Database password
        self.password = self.source_database_param.password

        self.auth = HTTPBasicAuth(self.user, self.password)
        self.columns_change_date_format = []
        self.endpoint = self.bronze_source_properties.table
        self.headers = self.source_database_param.headers
        self.params = self.source_database_param.params

        if self.bronze_source_properties.incremental:
            self.params[
                "sysparm_query"] = f"{self.bronze_source_properties.date_criteria}>{self.bronze_source_properties.last_update}"

        self.response = requests.get(self.url + self.endpoint, auth=self.auth, params=self.params)
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

        try:
            # Get json data
            self.response.raise_for_status()
            data = self.response.json()
            tmp = data.get('result', [])
            df = pd.DataFrame(tmp)

            return df

        except Exception as e:
            print(f"Error: An unexpected error occurred: {e}")
            return []

    def set_columns_to_transform(self, df : DataFrame) -> None:
        """Get columns from df where date format is str(yyy-MM-DD HH:MM:SS)"""

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

    def fetch_value_from_link(self, link : str):
        """Fetch value from link method"""

        try:
            request_link = requests.get(link, auth=self.auth, params=self.params)
            data_link = request_link.json()

            name = data_link.get('result', {}).get('name')

            if name:
                LINKS_CACHE[link] = name

                return name

            elif not name:
                number = data_link.get('result', {}).get('number')
                LINKS_CACHE[link] = number

                return number

            else:
                return get_final_segment(link)

        # Manage exception thrown
        except Exception as e:
            print(f"Error: HTTP invalid behavior occurred: {e} for URL: {link}")
            return get_final_segment(link)

    def update_link_to_value(self, df : DataFrame) -> None:
        """Update link to value method"""

        # Browse registered columns type dictionary
        for col in COLUMNS_TYPE_DICT:
            for value in df[col]:
                if value:
                    link = value['link']

                    if link in LINKS_CACHE:
                        df[col][value] = LINKS_CACHE[link]

                    else:
                        df[col][value] = self.fetch_value_from_link(str(link))

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

                        # TODO: Note : Voir si je peux pas check juste chaques variables du dict, voir si elle contient plus de 2 index, si l'un des deux index est link je fais la transfo, au lieu de checker tout les patterns
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

                        self.update_link_to_value(data)

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