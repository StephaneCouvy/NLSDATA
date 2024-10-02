import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *
import aiohttp
import asyncio
import time
import oracledb
from datetime import datetime

# "sysparm_limit": 1000,
# "sysparm_offset": 0

'''CHANGE_DATE_FORMAT = ['sys_updated_on', 'inc_sys_updated_on', 'md_sys_updated_on', 'mi_sys_updated_on',
                      'sys_created_on', 'closed_at', 'opened_at', 'business_duration', 'activity_due', 'sla_due'
                      'calendar_duration', 'requested_by_date', 'approval_set', 'end_date', 'work_start', 'start_date',
                      'work_end', 'conflict_last_run', 'resolved_at', 'u_duration_calc', 'reopened_time', 'u_state_changed_date',
                      'inc_u_duration_calc', 'mi_sys_created_on', 'inc_sys_created_on', 'inc_business_duration', 'due_date'
                      'inc_calendar_duration', 'md_sys_created_on', 'inc_opened_at', 'inc_resolved_at', 'inc_closed_at', 'mi_business_duration', 'mi_duration', 'mi_start', 'mi_end']
'''
LINKS_CACHE = {}
COLUMNS_TYPE_DICT = []

# Column's name where name is a SQL operation
RENAME_COLUMNS = ['number', 'order']


class BronzeSourceBuilderRestAPI(BronzeSourceBuilder):
    '''BronzeSourceBuilderRestAPI class'''

    def __init__(self, pSourceProperties: SourceProperties, pBronze_config: BronzeConfig,
                 pBronzeDb_Manager: BronzeDbManager, pLogger: BronzeLogger):
        '''BronzeSourceBuilderRestAPI constructor'''

        vSourceProperties = pSourceProperties._replace(type="REST_API")
        super().__init__(vSourceProperties, pBronze_config, pBronzeDb_Manager, pLogger)
        self.source_database_param = get_parser_config_settings("rest_api")(self.bronze_config.get_configuration_file(),
                                                                            self.get_bronze_source_properties().name)
        self.url = self.source_database_param.url
        self.user = self.source_database_param.user
        self.password = self.source_database_param.password
        self.headers = self.source_database_param.headers
        self.endpoint = self.bronze_source_properties.table
        self.params = self.source_database_param.params
        self.auth = HTTPBasicAuth(self.user, self.password)
        self.columns_change_date_format = []

        if self.bronze_source_properties.incremental:
            self.params[
                "sysparm_query"] = f"{self.bronze_source_properties.date_criteria}>{self.bronze_source_properties.last_update}"

        self.response = requests.get(self.url + self.endpoint, auth=self.auth, params=self.params)
        self.response_data = self.response.json()

        if self.response.status_code != 200:
            vError = "ERROR connecting to : {}".format(self.get_bronze_source_properties().name)
            raise Exception(vError)


    def get_bronze_row_lastupdate_date(self):
        '''Get Bronze row lasupdate date method'''

        if not self.bronze_date_lastupdated_row:
            v_dict_join = self.get_externaltablepartition_properties()._asdict()
            v_join = " AND ".join(
                [f"{INVERTED_EXTERNAL_TABLE_PARTITION_SYNONYMS.get(key, key)} = '{value}'" for key, value in
                 v_dict_join.items()])

            self.bronze_date_lastupdated_row = self.get_bronzedb_manager().get_bronze_lastupdated_row(self.bronze_table,
                                                                                                      self.bronze_source_properties.date_criteria,
                                                                                                      v_join)

        return self.bronze_date_lastupdated_row


    def __set_bronze_bucket_proxy__(self):
        '''Set Bronze bucket proxy method'''
        self.bronze_bucket_proxy.set_bucket_by_extension(p_bucket_extension=self.get_bronze_source_properties().name)


    def __set_bronze_table_settings__(self):
        '''Set Bronze bucket proxy method'''
        v_bronze_table_name = self.get_bronze_source_properties().bronze_table_name

        if not v_bronze_table_name:
            v_bronze_table_name = self.bronze_table = self.get_bronze_source_properties().name + "_" + self.get_bronze_source_properties().schema + "_" + self.get_bronze_source_properties().table.replace(
                " ", "_")

        self.bronze_table = v_bronze_table_name.upper()
        self.parquet_file_name_template = self.bronze_table
        self.parquet_file_id = 0
        v_dict_externaltablepartition = self.get_externaltablepartition_properties()._asdict()
        self.bucket_file_path = self.get_bronze_source_properties().schema + "/" + '/'.join(
            [f"{key}" for key in v_dict_externaltablepartition.values()]) + "/"
        self.parquet_file_id = self.__get_last_parquet_idx_in_bucket__()


    def fetch_chunk(self):
        '''Fetch chunk data method'''
    
        try:
            # Get json data
            self.response.raise_for_status()
            data = self.response.json()
            tmp = data.get('result', [])
            df = pd.DataFrame(tmp)
            
            return df

        except requests.RequestException as e:
            print(f"HTTP error occurred: {e}")
            return []
        
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return []


    def get_columns_to_transform(self, df):
        '''Get columns from df where date format is str(yyy-MM-DD HH:MM:SS)'''
        
        for col in df.columns:
            first_non_null_value = next((value for value in df[col] if pd.notnull(value)), None)

            if isinstance(first_non_null_value, str) and len(first_non_null_value) == 19:
                try:
                    # Test if conversion is possible
                    datetime.strptime(first_non_null_value, "%Y-%m-%d %H:%M:%S")
                    self.columns_change_date_format.append(col)
                
                except ValueError:
                    pass


    def transform_columns(self, df):
        '''Change date format, timezone, and columns where column's name is a SQL operation'''

        self.get_columns_to_transform(df)

        for col in self.columns_change_date_format:
            df[col] = df[col].str.replace('-', '/', regex=False)
            df[col] = pd.to_datetime(df[col], format='%Y/%m/%d %H:%M:%S')
            df[col] = df[col].dt.tz_localize('UTC')
            df[col] = df[col].dt.tz_convert('Europe/Paris')

        for col in df.columns:
            if col in RENAME_COLUMNS:
                df.rename(columns={col: f"{col}_id"}, inplace=True)

        return df


    def fetch_all_data(self):
        ''' Return a df with all incidents'''

        all_incidents_df = self.fetch_chunk()

        if all_incidents_df.empty:
            all_incidents_df = None
            print("No data fetched.")

        return all_incidents_df


    def get_columns_type_dict(self, df):
        '''Get columns type dict method'''
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                COLUMNS_TYPE_DICT.append(col)


    def get_final_segment(self, link):
        '''Get final segment method'''
        
        final_segment = link.rsplit('/', 1)[-1]

        if final_segment == 'global':
            LINKS_CACHE[link] = final_segment
            return final_segment

        else:
            LINKS_CACHE[link] = None
            return None


    def fetch_value_from_link(self, link):
        '''Fetch value from link method'''

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
                return self.get_final_segment(link)

        except aiohttp.ClientError as e:
            print(f"HTTP error occurred: {e} for URL: {link}")
            return self.get_final_segment(link)


    def update_link_to_value(self, df):
        '''Update link to value method'''
        for col in COLUMNS_TYPE_DICT:
            print(col)
            for value in df[col]:
                if value:
                    link = value['link']

                    if link in LINKS_CACHE:
                        df[col][value] = LINKS_CACHE[link]
                    else:
                        df[col][value] = self.fetch_value_from_link(link)

        return df


    def fetch_source(self, verbose=None):
        '''Create parquet file(s) from the source'''

        try:
            if self.response.status_code != 200:
                raise Exception("Error no DB connection")

            else:
                if verbose:
                    message = "Mode {2} : Extracting data {0},{1}".format(self.get_bronze_source_properties().schema,
                                                                          self.get_bronze_source_properties().table,
                                                                          SQL_READMODE)
                    verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message,
                                log_request=self.request + ': ' + str(self.db_execute_bind))

                self.df_table_content = pd.DataFrame()
                data = self.response.json()

                # TODO: manage different case between SERVICENOW and CPQ
                match self.get_bronze_source_properties().name:
                    case "SERVICE_NOW":
                        data = self.fetch_all_data()
                        data = self.transform_columns(data)
                        self.get_columns_type_dict(data)
                        start_time = time.time()
                        self.update_link_to_value(data)
                        end_time = time.time()

                        traitment_link_time = end_time - start_time
                        print(traitment_link_time)

                    case "CPQ":
                        data = data['items']

                self.df_table_content = pd.DataFrame(data)

                res = self.__create_parquet_file__(verbose)

                if not res:
                    raise Exception("Error creating parquet file")

                self.__update_fetch_row_stats__()
                elapsed = datetime.now() - self.fetch_start

                if verbose:
                    message = "{0} rows in {1} seconds".format(self.total_imported_rows, elapsed)
                    verbose.log(datetime.now(tz=timezone.utc), "FETCH", "RUN", log_message=message)

                return True

        except UnicodeDecodeError as err:
            vError = "ERROR Unicode Decode, table {}".format(self.get_bronze_source_properties().table)

            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err),
                            log_request=self.request)
            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()

            return False

        except oracledb.Error as err:
            vError = "ERROR Fetching table {}".format(self.get_bronze_source_properties().table)

            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError,
                            log_message='Oracle DB error: ' + str(err), log_request=self.request)
            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()

            return False

        except Exception as err:
            vError = "ERROR Fetching table {}".format(self.get_bronze_source_properties().table)

            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err),
                            log_request=self.request)
            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()

            return False
