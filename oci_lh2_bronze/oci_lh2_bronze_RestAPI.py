import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *
import aiohttp
import asyncio
import time
from datetime import datetime

CHANGE_DATE_FORMAT = ['sys_updated_on', 'sys_created_on', 'closed_at', 'opened_at', 'business_duration', 'calendar_duration', 'requested_by_date', 'approval_set', 'end_date', 'work_start', 'start_date', 'work_end', 'conflict_last_run', 'resolved_at', 'u_duration_calc', 'reopened_time']
RENAME_COLUMNS = ['number', 'order']

class BronzeSourceBuilderRestAPI(BronzeSourceBuilder):
    def __init__(self, pSourceProperties: SourceProperties, pBronze_config: BronzeConfig,
                 pBronzeDb_Manager: BronzeDbManager, pLogger: BronzeLogger):
        vSourceProperties = pSourceProperties._replace(type="REST_API")
        super().__init__(vSourceProperties, pBronze_config, pBronzeDb_Manager, pLogger)
        self.source_database_param = get_parser_config_settings("rest_api")(self.bronze_config.get_configuration_file(), self.get_bronze_source_properties().name)
        self.url = self.source_database_param.url
        self.user = self.source_database_param.user
        self.password = self.source_database_param.password
        self.headers = self.source_database_param.headers
        self.endpoint = self.bronze_source_properties.table
        self.params = self.source_database_param.params
        self.auth = aiohttp.BasicAuth(self.user, self.password)
        if self.isexternalpartionedtable():
            self.params["sysparm_query"] = f"{self.bronze_source_properties.date_criteria}>{self.bronze_source_properties.last_update}"
        self.response = requests.get(self.url + self.endpoint, auth=HTTPBasicAuth(self.user, self.password), headers=self.headers, params=self.params)

        self.cache = {}
        self.semaphore = asyncio.Semaphore(10)

        if self.response.status_code != 200:
            vError = "ERROR connecting to : {}".format(self.get_bronze_source_properties().name)
            raise Exception(vError)

    def get_bronze_row_lastupdate_date(self):
        if not self.bronze_date_lastupdated_row:
            v_dict_join = self.get_externaltablepartition_properties()._asdict()
            v_join= " AND ".join([f"{INVERTED_EXTERNAL_TABLE_PARTITION_SYNONYMS.get(key,key)} = '{value}'" for key, value in v_dict_join.items()])
            self.bronze_date_lastupdated_row = self.get_bronzedb_manager().get_bronze_lastupdated_row(self.bronze_table, self.bronze_source_properties.date_criteria,v_join)
        return self.bronze_date_lastupdated_row

    def __set_bronze_bucket_proxy__(self):
        self.bronze_bucket_proxy.set_bucket_by_extension(p_bucket_extension=self.get_bronze_source_properties().name)

    def __set_bronze_table_settings__(self):
        v_bronze_table_name = self.get_bronze_source_properties().bronze_table_name
        if not v_bronze_table_name:
            v_bronze_table_name = self.bronze_table = self.get_bronze_source_properties().name + "_" + self.get_bronze_source_properties().schema + "_" + self.get_bronze_source_properties().table.replace(" ", "_")
        self.bronze_table = v_bronze_table_name.upper()
        self.parquet_file_name_template = self.bronze_table
        self.parquet_file_id = 0
        v_dict_externaltablepartition = self.get_externaltablepartition_properties()._asdict()
        self.bucket_file_path = self.get_bronze_source_properties().schema + "/" + '/'.join(
            [f"{key}" for key in v_dict_externaltablepartition.values()]) + "/"
        self.parquet_file_id = self.__get_last_parquet_idx_in_bucket__()

    async def fetch_name_from_link(self, session, link, retries=3):
        if link in self.cache:
            return self.cache[link]

        attempt = 0
        while attempt < retries:
            async with self.semaphore:
                try:
                    async with session.get(link, auth=self.auth) as response:
                        content_type = response.headers.get('Content-Type', '')
                        if 'application/json' not in content_type:
                            content = await response.text()
                            print(f"Unexpected content type for {link}. Response content: {content}")
                            final_segment = link.rsplit('/', 1)[-1]
                            self.cache[link] = final_segment
                            return final_segment

                        response_data = await response.json()
                        if response_data.get('result', {}).get('name'):
                            name = response_data.get('result', {}).get('name')
                            if name is not None:
                                self.cache[link] = name
                                return name
                        elif response_data.get('result', {}).get('number'):
                            number = response_data.get('result', {}).get('number')
                            self.cache[link] = number
                            return number
                        else:
                            final_segment = link.rsplit('/', 1)[-1]
                            if final_segment == 'global':
                                self.cache[link] = final_segment
                                return final_segment
                            else:
                                self.cache[link] = None
                                return None
                            
                except aiohttp.ClientError as e:
                    print(f"HTTP error occurred: {e} for URL: {link}")
                    attempt += 1
                    if attempt < retries:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        final_segment = link.rsplit('/', 1)[-1]
                        self.cache[link] = final_segment
                        return final_segment

        final_segment = link.rsplit('/', 1)[-1]
        self.cache[link] = final_segment
        return final_segment

    async def transform_data(self, session, df):
        dict_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, dict)).any()]

        for col in dict_columns:
            df[col] = await asyncio.gather(
                *[self.fetch_name_from_link(session, value['link']) if isinstance(value, dict) and 'link' in value else asyncio.sleep(0, result=value) for value in df[col]]
            )

        return df

    async def fetch_chunk(self, session, offset, limit):
        self.params['sysparm_offset'] = offset
        self.params['sysparm_limit'] = limit
        try:
            async with session.get(self.url + self.endpoint, auth=self.auth, params=self.params) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get('result', [])
        except aiohttp.ClientError as e:
            print(f"HTTP error occurred: {e}")
            return []
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return []

    async def fetch_all_incidents(self):
        all_incidents_df = pd.DataFrame()
        offset = 0
        limit = self.params['sysparm_limit']

        async with aiohttp.ClientSession() as session:
            while True:
                start_time = time.time()
                chunk = await self.fetch_chunk(session, offset, limit)

                if not chunk:
                    break

                chunk_df = pd.DataFrame(chunk)
                chunk_df = await self.transform_data(session, chunk_df)
                all_incidents_df = pd.concat([all_incidents_df, chunk_df], ignore_index=True)
                end_time = time.time()

                chunk_time = end_time - start_time
                offset += limit

                print(f"Fetched {len(chunk)} incidents, total incidents so far: {len(all_incidents_df)}")
                print(f"Time for this chunk: {chunk_time:.2f} seconds")

        return all_incidents_df

    async def main(self):
        all_incidents_df = await self.fetch_all_incidents()

        if all_incidents_df.empty:
            print("No incidents fetched.")

        #df = all_incidents_df.iloc[:, :90]
        for col in all_incidents_df.columns:
            if col in CHANGE_DATE_FORMAT:
                all_incidents_df[col] = all_incidents_df[col].str.replace('-', '/', regex=False)
                all_incidents_df[col] = pd.to_datetime(all_incidents_df[col], format='%Y/%m/%d %H:%M:%S')

        for col in all_incidents_df.columns:
            if col in RENAME_COLUMNS:
                all_incidents_df.rename(columns={col: f"{col}_id"}, inplace=True)

        return all_incidents_df

    def fetch_source(self, verbose=None):
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

                match self.get_bronze_source_properties().name:
                    case "SERVICE_NOW":
                        data = asyncio.run(self.main())
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
            self.logger.log(p_error=err, p_action=vError)
            self.__update_fetch_row_stats__()
            return False
        except oracledb.Error as err:
            vError = "ERROR Fetching table {}".format(self.get_bronze_source_properties().table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError,
                            log_message='Oracle DB error: ' + str(err), log_request=self.request)
            self.logger.log(p_error=err, p_action=vError)
            self.__update_fetch_row_stats__()
            return False
        except Exception as err:
            vError = "ERROR Fetching table {}".format(self.get_bronze_source_properties().table)
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err),
                            log_request=self.request)
            self.logger.log(p_error=err, p_action=vError)
            self.__update_fetch_row_stats__()
            return False