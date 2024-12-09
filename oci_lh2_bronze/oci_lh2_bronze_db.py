import oracledb
import pandas as pd

from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *


class BronzeSourceBuilderDb(BronzeSourceBuilder):
    '''BronzeSourceBuilder DB class'''

    def __init__(self, pSourceProperties:SourceProperties, pBronze_config:BronzeConfig, pBronzeDb_Manager:BronzeDbManager, pLogger:BronzeLogger):
        '''BronzeSourceBuilder DB constructor'''

        vSourceProperties = pSourceProperties._replace(type="DB")
        super().__init__(vSourceProperties, pBronze_config, pBronzeDb_Manager,pLogger)

        # Create connexion to source database
        self.source_database_param = get_parser_config_settings("database")(self.bronze_config.get_configuration_file(), self.get_bronze_source_properties().name)
        self.source_db = DBFACTORY.create_instance(self.source_database_param.dbwrapper, self.bronze_config.get_configuration_file())
        self.source_db_connection = self.source_db.create_db_connection(self.source_database_param)

        if not self.source_db_connection:
            vError = "ERROR connecting to : {}".format(self.get_bronze_source_properties().name)
            raise Exception(vError)

        # Customize select to force encode of columns
        self.__custom_select_from_source__()


    def get_bronze_row_lastupdate_date(self):
        '''Get Bronze row last update date method'''
    
        if not self.bronze_date_lastupdated_row:
            v_dict_join = self.get_externaltablepartition_properties()._asdict()
            v_join= " AND ".join([f"{INVERTED_EXTERNAL_TABLE_PARTITION_SYNONYMS.get(key,key)} = '{value}'" for key, value in v_dict_join.items()])
            self.bronze_date_lastupdated_row = self.get_bronzedb_manager().get_bronze_lastupdated_row(self.bronze_table, self.bronze_source_properties.date_criteria, v_join)
    
        return self.bronze_date_lastupdated_row
    
    
    def get_source_table_indexes(self):
        '''Get source table indexes if not already defined'''

        if not self.source_table_indexes:
            v_dict_source_table_indexes = self.source_db.get_table_indexes(self.get_bronze_source_properties().table)
            v_source_table_indexes =  dict_to_string(v_dict_source_table_indexes)
            self.source_table_indexes = v_source_table_indexes.replace('\'', '') if v_source_table_indexes else None

        return self.source_table_indexes
    

    def get_bronze_bis_pk(self):
        '''List columns of primary key based on source table indexes : search for unique index unique index name ends with _U{digit} or _PK'''
        
        if not self.bronze_bis_pk:
            # Convert string with tables indexes to dictionnary
            v_dict_tables_indexes = convert_string_to_dict(self.get_source_table_indexes())
            v_pattern = re.compile(r'.*_(U\d+|PK)$')
            v_key = None
            
            # Search key (index name) ends with _U{digit} or _PK
            if v_dict_tables_indexes:
                for key in v_dict_tables_indexes:
                    if v_pattern.match(key):
                        v_key = key
                        break
            
            # Get list of index columns
            v_list_columns_index = v_dict_tables_indexes[v_key] if v_key else None
            
            # Build join for merge between source table S and target table T
            v_pk_columns = ",".join([f"{value}" for value in v_list_columns_index]) if v_list_columns_index else None
            self.bronze_bis_pk = v_pk_columns
    
        return self.bronze_bis_pk
    

    def __set_bronze_bucket_proxy__(self):
        '''define settings for bucket, especially storagename... could depends on subclass'''
        self.bronze_bucket_proxy.set_bucket_by_extension(p_bucket_extension=self.get_bronze_source_properties().name)
    

    def __set_bronze_table_settings__(self):
        '''define bronze table name, bucket path to add parquet files, get index to restart parquet files interation'''

        v_bronze_table_name = self.get_bronze_source_properties().bronze_table_name

        if not v_bronze_table_name:
            v_bronze_table_name = self.bronze_table = self.get_bronze_source_properties().name + "_" + self.get_bronze_source_properties().schema + "_" + self.get_bronze_source_properties().table.replace(" ", "_")
        
        self.bronze_table = v_bronze_table_name.upper()
        
        # Define template name of parquet files
        #self.parquet_file_name_template = self.get_bronze_source_properties().name + "_" + self.get_bronze_source_properties().table.replace(" ", "_")
        self.parquet_file_name_template = self.bronze_table
        self.parquet_file_id = 0
        
        # Define the path for storing parquets files in the bucket
        #self.bucket_file_path = self.get_bronze_source_properties().schema + "/" + self.year + "/" + self.month + "/" + self.day + "/"
        v_dict_externaltablepartition = self.get_externaltablepartition_properties()._asdict()
        self.bucket_file_path = self.get_bronze_source_properties().schema + "/" + '/'.join([f"{key}" for key in v_dict_externaltablepartition.values()]) + "/"
        
        # Get the index of the last Parquet file in the bucket
        self.parquet_file_id = self.__get_last_parquet_idx_in_bucket__()
        

    def fetch_source(self,verbose=None):
        '''Fetch source method
        Create parquet file depending on data source (dbcursor, pandas.dataframe)
        '''

        try:
            if not self.source_db_connection:
                raise Exception("Error no DB connection")

            # Execute a SQL query to fetch all data from the current table
            if verbose:
                message = "Mode {2} : Extracting data {0},{1}".format(self.get_bronze_source_properties().schema, self.get_bronze_source_properties().table, SQL_READMODE)
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message, log_request = self.request + ': ' + str(self.db_execute_bind))

            self.df_table_content = pd.DataFrame()

            match SQL_READMODE:
                case "DBCURSOR":
                    cursor = self.source_db_connection.cursor()
                    cursor.arraysize = DB_ARRAYSIZE

                    if self.db_execute_bind:
                        # where clause on date
                        cursor.execute(self.request, self.db_execute_bind)
                    else:
                        cursor.execute(self.request)

                    while True:
                        # Fetch data by chunk and convert to dataframe
                        # chunk specified by DB_ARREYSIZE
                        rows = cursor.fetchmany()

                        # Map column name from cursor to df ; force types of columns based on cursor type
                        self.df_table_content = self.source_db.create_df_with_db_cursor(rows, cursor.description)
                        
                        if self.df_table_content.empty:
                            break
                        
                        # Create parquet file for current chunk dataframe
                        res = self.__create_parquet_file__(verbose)
                        
                        if not res:
                            raise Exception("Error creating parquet file")
                        
                        # Update total count of imported rows
                        self.__update_fetch_row_stats__()
                        elapsed = datetime.now() -self.fetch_start
                        
                        if verbose:
                            message = "{0} rows in {1} seconds".format(self.total_imported_rows, elapsed)
                            verbose.log(datetime.now(tz=timezone.utc), "FETCH", "RUN", log_message=message)
  
                    cursor.close()
                
                case "PANDAS":
                    # Fetch data by chunk and convert to dataframe
                    for df_chunk in pd.read_sql(sql=self.request, con=self.source_db_connection, chunksize=PANDAS_CHUNKSIZE):
                        self.df_table_content = pd.concat([self.df_table_content, df_chunk])
                        
                        # Create parquet file for current chunk dataframe
                        res = self.__create_parquet_file__(verbose)
                        
                        if not res:
                            raise Exception("Error creating parquet file")
                        
                        # Update total count of imported rows
                        self.__update_fetch_row_stats__()
                        elapsed = datetime.now() - self.fetch_start
                        
                        if verbose:
                            message = "{0} rows in {1} seconds".format(self.total_imported_rows, elapsed)
                            verbose.log(datetime.now(tz=timezone.utc), "FETCH", "RUN", log_message=message)
        
            return True
        
        # Catching Unicode Decode Error
        except UnicodeDecodeError as err:  
            vError = "ERROR Unicode Decode, table {}".format(self.get_bronze_source_properties().table)
        
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err), log_request=self.request)
        
            self.logger.log(p_error=err, p_action=vError)
            self.__update_fetch_row_stats__()
        
            return False
        
        except oracledb.Error as err:
            vError = "ERROR Fetching table {}".format(self.get_bronze_source_properties().table)
        
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message='Oracle DB error :{}'.format(str(err)), log_request=self.request)
        
            self.logger.log(p_error=err, p_action=vError)
            self.__update_fetch_row_stats__()
        
            return False
        
        except Exception as err:
            vError = "ERROR Fetching table {}".format(self.get_bronze_source_properties().table)
        
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err), log_request=self.request)
        
            self.logger.log(p_error=err, p_action=vError)
            self.__update_fetch_row_stats__()
        
            return False