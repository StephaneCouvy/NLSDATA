import os.path
import oracledb
import pandas as pd
from nlsdata.nlsdata_utils import *
from nlsdb.dbwrapper_factory import *
from nlsfilestorage.filestorage_wrapper.abs_filestorage import *
from nlsfilestorage.filestorage_wrapper_factory import *

OPTION_ARG_LOADING_TABLE = 'l'
OPTION_ARG_LOG_TABLE = 'o'
OPTION_ARG_RELOAD_ON_ERROR_INTERVAL = 'x'
OPTION_ARG_NOT_DROP_TEMP_RUNNING_LOADING_TABLE = 'k'
OPTION_ARG_QUERY = 'q'
OPTION_ARG_SIMULATE = 'simulate'
OPTION_ARG_NOT_PRE_PROC ='not_pre_proc'
OPTION_ARG_NOT_POST_PROC ='not_post_proc'

ZOMBIES_TABLE_NAME = 'ZOMBIES'
#RESET_DATE_LASTUPDATE = datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

FILESTORAGE_BRONZE_BUCKET_DEBUG = 'BRONZE_BUCKET_DEBUG'
FILESTORAGE_BRONZE_BUCKET = 'BRONZE_BUCKET'

PARQUET_IDX_DIGITS = 5
PANDAS_CHUNKSIZE = 100000

# Variables could be redefined by json config file
DB_ARRAYSIZE = 50000
SQL_READMODE = "DBCURSOR" #CURSORDB / PANDAS
PARQUET_FILE_EXTENSION = ".parquet"

DBFACTORY = NLSDbFactory()
FILESTORAGEFACTORY = NLSFileStorageFactory()

DICT_STATUS_CODE = {'completed':'COMPLETED', 'error':'ERROR', 'data_fetched':'FETCHED', 'parquet_sent':'UPLOADED', 'bronze_updated':'UPDATED', "fetching":"FETCHING","dropped":"DROPPED","copying_parquets":"COPYING","parquets_copied":"COPIED"}
SOURCE_PROPERTIES_SYNONYMS = {'SRC_TYPE': 'type', 'SRC_NAME': 'name', 'SRC_ORIGIN_NAME': 'schema', 'SRC_OBJECT_NAME': 'table', 'SRC_OBJECT_CONSTRAINT': 'table_constraint', 'SRC_FLAG_ACTIVE': 'active', 'SRC_FLAG_INCR': 'incremental', 'SRC_DATE_CONSTRAINT': 'date_criteria', 'SRC_DATE_LASTUPDATE': 'last_update', 'FORCE_ENCODE': 'force_encode', 'BRONZE_BIS_PK':'bronze_bis_pk','BRONZE_POST_PROCEDURE': 'bronze_post_proc', 'BRONZE_POST_PROCEDURE_ARGS': 'bronze_post_proc_args','BRONZE_TABLE_NAME':'bronze_table_name','BRONZE_LASTUPLOADED_PARQUET':'lastuploaded_parquet','SRC_TABLE_IDX':'source_table_indexes','RESET_LASTUPDATE':'reset_lastupdate','BRONZE_UPDATE':'bronze_update'}
INVERTED_SOURCE_PROPERTIES_SYNONYMS = {value: key for key, value in SOURCE_PROPERTIES_SYNONYMS.items()}
SourceProperties = namedtuple('SourceProperties',list(SOURCE_PROPERTIES_SYNONYMS.values()))

EXTERNAL_TABLE_PARTITION_SYNONYMS = {'FETCH_YEAR':'year','FETCH_MONTH':'month','FETCH_DAY':'day'}
INVERTED_EXTERNAL_TABLE_PARTITION_SYNONYMS = {value: key for key, value in EXTERNAL_TABLE_PARTITION_SYNONYMS.items()}
ExternalTablePartitionsProperties = namedtuple('ExternalTablePartitionsProperties',list(EXTERNAL_TABLE_PARTITION_SYNONYMS.values()))

# SourceProperties namedtuple is set into Exploit __init__, based on fields of table used to list sources
BronzeProperties = namedtuple('BronzeProperties', ['environment', 'schema', 'table', 'bucket', 'bucket_filepath', 'parquet_template'])


# noinspection PyRedundantParentheses
class BronzeConfig():
    '''Define configuration envrionment parameters to execute.
    Mainly extract parameters from json configuration_file
    Provide a generic method to get an oracledb connection, depending on database options defined into json
    '''

    def __init__(self, configuration_file):
        '''BronzeConfig constructor'''
        
        self.configuration_file = path_replace_tilde_with_home(configuration_file)
        self.options = get_parser_config_settings("options")(self.configuration_file, "options")
        
        self.duckdb_settings = get_parser_config_settings("duckdb_settings")(self.configuration_file, "duckdb_settings")
        
        if self.options.db_arraysize.isdigit():
            global DB_ARRAYSIZE
            DB_ARRAYSIZE = eval(self.options.db_arraysize)

        if not self.options.sql_readmode:
            global SQL_READMODE
            SQL_READMODE = self.options.sql_readmode

        if self.options.environment == "DEBUG":
            self.debug = True
            self.oci_settings = get_parser_config_settings("filestorage")(self.configuration_file, FILESTORAGE_BRONZE_BUCKET_DEBUG)
        else:
            self.debug = False
            self.oci_settings = get_parser_config_settings("filestorage")(self.configuration_file, FILESTORAGE_BRONZE_BUCKET)

        if self.options.rootdir != '':
            self.rootdir = path_replace_tilde_with_home(self.options.rootdir)
            os.chdir(self.rootdir)
        else:
            self.rootdir = ''

        # Create a temporary directory if it doesn't exist
        self.tempdir = self.options.tempdir

        if not os.path.exists(self.tempdir):
            os.makedirs(self.tempdir)

        # Return absolute Path for log file.
        # If full path is not specified, then add workging dir ahead
        if not os.path.dirname(self.get_options().verboselogfile):
            self.verboselogfile = os.path.join(self.get_tempdir(),self.get_options().verboselogfile)
        else:
            self.verboselogfile = self.get_options().verboselogfile
        
        self.verboselevel = self.get_options().verbose_level


    def get_configuration_file(self):
        '''Get configuration file method'''
        return self.configuration_file

    
    def get_options(self):
        '''Get options method'''
        return self.options

    
    def get_duckdb_settings(self):
        '''Get duckDB setting method'''
        return self.duckdb_settings

    
    def get_oci_settings(self):
        '''Get OCI settings method'''
        return self.oci_settings
            
    
    def isdebugmode(self):
        '''Is debug mode method'''
        return self.debug

    
    def get_rootdir(self):
        '''Get rootdir method'''
        return self.rootdir
    
    def get_tempdir(self):
        '''Get temp directory method'''
        return self.tempdir
    

    def get_verboselogfile(self):
        '''Get verbose log file method'''
        return self.verboselogfile
    
    
    def get_verboselevel(self):
        '''Get verbose level method'''
        return self.verboselevel


# noinspection PyRedundantParentheses
class BronzeLogger():
    '''BronzeLogger method'''

    def __init__(self, pBronze_config, p_verbose=None):
        '''BronzeLogger constructor'''

        self.logger_linked_bronze_source = None
        self.logger_linked_bronze_config = pBronze_config
        self.verbose = p_verbose
        self.logger_oracledb_connection = None
        
        if self.logger_linked_bronze_config:
            self.logger_env = pBronze_config.get_options().environment
            self.logger_table_name = self.logger_linked_bronze_config.get_options().log_table_prefix + self.logger_linked_bronze_config.get_options().environment
        
        self._init_logger()


    def _init_logger(self):
        '''Init logger method'''
    
        if self.logger_linked_bronze_config:
            # Establish a connection to EXPLOIT schema to log message
            self.logger_db_param = get_parser_config_settings("database")(self.logger_linked_bronze_config.get_configuration_file(),
                                                                           self.logger_linked_bronze_config.get_options().logger_db)
            self.logger_db:absdb = DBFACTORY.create_instance(self.get_db_parameters().dbwrapper,self.logger_linked_bronze_config.get_configuration_file())
            self.logger_oracledb_connection = self.get_db().create_db_connection(self.get_db_parameters())

            # Set Process_info = hostname:user:pid
            vProcess_info = "{}:{}:{}".format(socket.gethostname(),os.getlogin(),os.getpid())
            self.BronzeLoggerProperties = self.get_db().create_namedtuple_from_table('BronzeLoggerProperties', self.logger_table_name)
            
            #self.instance_bronzeloggerproperties = self.BronzeLoggerProperties(START_TIME=datetime.now(tz=timezone.utc),END_TIME=None, ENVIRONMENT=self.env,ACTION='',SRC_NAME='',SRC_ORIGIN_NAME='',SRC_OBJECT_NAME='',REQUEST='',ERROR_TYPE='',ERROR_MESSAGE='',STAT_ROWS_COUNT=0,STAT_ROWS_SIZE=0,STAT_TOTAL_DURATION=0,STAT_FETCH_DURATION=0,STAT_UPLOAD_PARQUETS_DURATION=0,STAT_SENT_PARQUETS_COUNT=0,STAT_SENT_PARQUETS_SIZE=0)
            self.instance_bronzeloggerproperties = self.BronzeLoggerProperties(START_TIME=datetime.now(tz=timezone.utc), ENVIRONMENT=self.logger_env, PROCESS_INFO=vProcess_info)


    def get_db(self):
        '''Get DB method'''
        return self.logger_db


    def get_db_parameters(self):
        '''Get DB parameters method'''
        return self.logger_db_param


    def get_db_connection(self):
        '''Get DB connection method'''
        return self.logger_oracledb_connection


    def link_to_bronze_source(self, pBronze_source):
        '''Link to bronze source method'''

        self.logger_linked_bronze_source:BronzeSourceBuilder = pBronze_source
        self.logger_linked_bronze_config:BronzeConfig = pBronze_source.get_bronze_config()
        self.logger_env = self.logger_linked_bronze_source.get_bronze_properties().environment
        vSourceProperties = self.logger_linked_bronze_source.get_bronze_source_properties()
        self._init_logger()
        self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(SRC_NAME=vSourceProperties.name, SRC_ORIGIN_NAME=vSourceProperties.schema, SRC_OBJECT_NAME=vSourceProperties.table)


    def set_logger_properties(self, p_src_name, p_src_origin_name, p_src_object_name, p_request="", p_duration=0):
        '''Set logger properties method'''
        self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(SRC_NAME=p_src_name, SRC_ORIGIN_NAME=p_src_origin_name, SRC_OBJECT_NAME=p_src_object_name, REQUEST=p_request, STAT_TOTAL_DURATION=p_duration)


    def get_log_table(self):
        '''Get log table method'''
        return self.logger_table_name


    def log(self,pAction="COMPLETED", pError=None):
        '''Log method'''

        v_action = pAction
        
        if pError:
            v_error_type = type(pError).__name__
            v_error_message = str(pError)
            
            # If error message contains warning then change action -> WARNING
            if re.match("WARNING",v_error_message.upper()):
                VAction = "WARNING"

        else:
            v_error_type = ''
            v_error_message = ''
        
        if self.logger_linked_bronze_source:
            v_source_durations_stats = self.logger_linked_bronze_source.get_durations_stats()
            v_source_rows_stats = self.logger_linked_bronze_source.get_rows_stats()
            v_source_parquets_stats = self.logger_linked_bronze_source.get_parquets_stats()
            v_source_properties = self.logger_linked_bronze_source.get_bronze_source_properties()
            v_source_lastupdated_row = self.logger_linked_bronze_source.get_bronze_row_lastupdate_date()
            v_source_bucket_parquet_files_sent = list_to_string(self.logger_linked_bronze_source.get_bucket_parquet_files_sent())
            
            self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(
                REQUEST=v_source_properties.request, ACTION=v_action, END_TIME=datetime.now(tz=timezone.utc), ERROR_TYPE=v_error_type, ERROR_MESSAGE=v_error_message,
                STAT_ROWS_COUNT=v_source_rows_stats[0], STAT_ROWS_SIZE=v_source_rows_stats[1],
                STAT_SENT_PARQUETS_COUNT=v_source_parquets_stats[0], STAT_SENT_PARQUETS_SIZE=v_source_parquets_stats[1],
                STAT_TOTAL_DURATION=v_source_durations_stats[0], STAT_FETCH_DURATION=v_source_durations_stats[1],
                STAT_UPLOAD_PARQUETS_DURATION=v_source_durations_stats[2], STAT_TEMP_PARQUETS_COUNT=v_source_parquets_stats[2],
                ATTRIBUTE1=v_source_lastupdated_row,
                ATTRIBUTE6=v_source_bucket_parquet_files_sent
            )

        else:
            self.instance_bronzeloggerproperties = self.instance_bronzeloggerproperties._replace(
                ACTION=v_action, END_TIME=datetime.now(tz=timezone.utc), ERROR_TYPE=v_error_type, ERROR_MESSAGE=v_error_message
            )
            
        self.__insertlog__()


    def __insertlog__(self):
        '''Insert log method'''
        self.get_db().insert_namedtuple_into_table(self.instance_bronzeloggerproperties, self.logger_table_name)


class BronzeExploit:
    '''Iterator object for list of sources to be imported into Bronze
    Define metohd to update src_dat_lastupdate for table with incremental integration
    '''

    def __init__(self, p_bronze_config:BronzeConfig, p_logger:BronzeLogger, p_verbose=None, **p_optional_args):
        '''BronzeExploit method'''

        self.idx = 0
        self.verbose = p_verbose
        self.logger = p_logger
        self.exploit_config = p_bronze_config
        self.exploit_db_param = get_parser_config_settings("database")(self.exploit_config.get_configuration_file(),"exploit")
        self.exploit_db:absdb = DBFACTORY.create_instance(self.exploit_db_param.dbwrapper, self.exploit_config.get_configuration_file())
        self.exploit_db_connection = self.exploit_db.create_db_connection(self.exploit_db_param)

        v_cursor = self.get_db_connection().cursor()

        # Drop temporary running loding table or Not (Default) 
        self.not_drop_running_loading_table = p_optional_args.get(OPTION_ARG_NOT_DROP_TEMP_RUNNING_LOADING_TABLE, True)

        # Create running/temporary List Datasource loading table.
        # Insert list of tables to import

        self.exploit_loading_table = self.exploit_config.get_options().datasource_load_tablename_prefix + self.exploit_config.get_options().environment
        self.exploit_running_loading_table = format_temporary_tablename(self.exploit_loading_table)
        v_message = "Create running exploit loading table  {}".format(self.exploit_running_loading_table)
        
        if self.verbose:
            self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "START", log_message=v_message)
            
        if p_optional_args.get(OPTION_ARG_LOADING_TABLE, None):
            self.batch_loading_table = p_optional_args[OPTION_ARG_LOADING_TABLE]
        else:    
            self.batch_loading_table = self.exploit_loading_table
        
        v_log_table = p_optional_args.get(OPTION_ARG_LOG_TABLE, None)
        v_interval_start = ''
        v_interval_end = ''
        
        if p_optional_args.get(OPTION_ARG_RELOAD_ON_ERROR_INTERVAL, None) :
            v_interval_start = p_optional_args[OPTION_ARG_RELOAD_ON_ERROR_INTERVAL][0].strftime("%Y-%m-%d %H:%M:%S")
            
            # If end date of intervant not provided, then take Now date
            try:
                v_interval_end = p_optional_args[OPTION_ARG_RELOAD_ON_ERROR_INTERVAL][1].strftime("%Y-%m-%d %H:%M:%S")
            except IndexError:
                v_interval_end = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            v_message = "Populate exploit loading table {} with previous error tables from log table {} on interval {}->{}".format(self.exploit_running_loading_table, v_log_table, v_interval_start, v_interval_end)
            
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "START", log_message=v_message)
       
        v_dmbs_output = self.get_db().execute_proc('LH2_EXPLOIT_ADMIN_PKG.CREATE_LH2_DATASOURCE_LOADING_PROC',*[self.exploit_loading_table, self.batch_loading_table, self.exploit_running_loading_table, v_log_table, v_interval_start, v_interval_end])
        self.get_db_connection().commit()
        
        if not self.get_db().last_execute_proc_completion():
            v_message = "ERROR, Create running exploit loading table  {}".format(self.exploit_running_loading_table)
            v_message += "\n{}".format(self.get_db().last_execute_proc_output())
            
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "ERROR", log_message=v_message)
            raise Exception(v_message)
        
        #Define SourceProperties namedtuple as a global type
        global SourceProperties
        
        v_loadingTableProperties = self.get_db().create_namedtuple_from_table('SourceProperties', self.exploit_running_loading_table)
        v_new_fields = [SOURCE_PROPERTIES_SYNONYMS.get(old_name,old_name) for old_name in v_loadingTableProperties._fields]
        v_new_fields.append('request')
        v_defaults_values = [None] * len(v_new_fields)
        SourceProperties = namedtuple ('SourceProperties', v_new_fields, defaults=v_defaults_values)
        
        # Execute a SQL query to fetch activ data from the table "LIST_DATASOURCE_LOADING_..." into a dataframe
        v_sql = "select * from " + self.exploit_running_loading_table + " where SRC_FLAG_ACTIV = 1 ORDER BY SRC_TYPE,SRC_NAME,SRC_OBJECT_NAME"
        v_cursor.execute(v_sql)
        v_df_exploit_datasource = pd.DataFrame(v_cursor.fetchall())
        v_df_exploit_datasource.columns = [x[0] for x in v_cursor.description]
        
        # Test if query set to filter data sources
        v_query = p_optional_args.get(OPTION_ARG_QUERY, None)
        
        if p_optional_args.get(OPTION_ARG_QUERY, None) :
            self.df_exploit_datasource_loading = create_filter_mask(v_df_exploit_datasource, v_query)
            
            if self.df_exploit_datasource_loading is None:
                raise Exception("ERROR, Filtering loading tables list, review your query")
        
        else:
            self.df_exploit_datasource_loading = v_df_exploit_datasource
        
        v_datasource_loading_list = self.df_exploit_datasource_loading['SRC_OBJECT_NAME'].tolist()
        
        if p_verbose:
            v_log_message = "{} data sources to load : {}".format(len(v_datasource_loading_list), v_datasource_loading_list)
            p_verbose.log(datetime.now(tz=timezone.utc), "DATASOURCE_LOADING", "STARTING", log_message=v_log_message)

        v_cursor.close()


    def __del__(self):
        '''Del method'''
        if not self.not_drop_running_loading_table:
            message = "Deleting temporary Exploit table  {}".format(self.exploit_running_loading_table)
            
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "END", log_message=message)
            
            self.get_db().execute_proc('LH2_EXPLOIT_ADMIN_PKG.DROP_TABLE_PROC', *[self.exploit_running_loading_table])
            message += "{}".format(self.get_db().last_execute_proc_output())
            
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "EXPLOIT", "END", log_message=message)
   
    def __iter__(self):
        '''Reset the index whenever a new iterator is requested'''
        self.idx = 0
        return self


    def __next__(self):
        '''Next method'''

        try:
            #items = [self.df_param.iloc[self.idx,i] for i in range(len(self.df_param.columns))]
            v_line = self.df_exploit_datasource_loading.iloc[self.idx].tolist()
            v_items = SourceProperties(*v_line)

        except IndexError:
            raise StopIteration()

        self.idx += 1

        return v_items


    def __getitem__(self, p_key):
        '''Get item method'''

        v_df_filtered_data = self.df_exploit_datasource_loading[self.df_exploit_datasource_loading['BRONZE_TABLE_NAME'] == p_key]
        
        if v_df_filtered_data.empty:
            raise KeyError(f"Key '{p_key}' not found")
        
        v_line = v_df_filtered_data.iloc[0].tolist()
        v_items = SourceProperties(*v_line)
        
        return v_items


    def __setitem__(self, p_key, p_value):
        '''Set item method'''
    
        v_index = self.data[self.df_exploit_datasource_loading['BRONZE_TABLE_NAME'] == p_key].index
        
        if not v_index.empty:
            self.df_exploit_datasource_loading.loc[v_index[0], self.df_exploit_datasource_loading.columns != 'BRONZE_TABLE_NAME'] = p_value
        else:
            v_new_row = pd.Series(p_value, index=self.df_exploit_datasource_loading.columns.drop('BRONZE_TABLE_NAME'))
            v_new_row['BRONZE_TABLE_NAME'] = p_key
            self.df_exploit_datasource_loading = self.data.append(v_new_row, ignore_index=True)


    def items(self):
        '''Items method'''

        for index, row in self.df_exploit_datasource_loading.iterrows():
            v_line = row.tolist()
            v_items = SourceProperties(*v_line)
            yield row['BRONZE_TABLE_NAME'], v_items


    def keys(self):
        '''Keys method'''
        return self.df_exploit_datasource_loading['BRONZE_TABLE_NAME']


    def values(self):
        '''Values method'''

        for index, row in self.df_exploit_datasource_loading.iterrows():
            v_line = row.tolist()
            v_items = SourceProperties(*v_line)
            yield v_items


    def get_db(self) -> absdb:
        '''Get DB method'''
        return self.exploit_db
    

    def get_db_parameters(self):
        '''Get DB parameters'''
        return self.exploit_db_param
    

    def get_db_connection(self):
        '''Get DB connection'''
        return self.exploit_db_connection
    

    def get_loading_tables(self):
        '''Get loading tables'''
        return (self.exploit_loading_table, self.exploit_running_loading_table)


    def update_exploit(self, p_dict_column_name_value, p_source:SourceProperties=None, p_bronze_table_name:str=None,p_simulate=False):
        '''Update exploit table with column and values provided into p_dict_column_name_value {'column1':value1,'column2':value2....}
        update is done on Source : Database name, schema, table
        '''

        v_request = ""

        try:
            v_cursor = self.get_db_connection().cursor()
            
            # Build SQL UPDATE SQL request
            v_request = "UPDATE " + self.exploit_loading_table + " "
            
            #v_request = "UPDATE " + self.exploit_loading_table + " SET "+p_column_name+" = :1 WHERE SRC_NAME = :2 AND SRC_ORIGIN_NAME = :3 AND SRC_OBJECT_NAME = :4"
            v_offset = 1
            v_set = "SET "+", ".join([f"{INVERTED_SOURCE_PROPERTIES_SYNONYMS.get(column,column)} =:{i+v_offset}" for i, column in enumerate(p_dict_column_name_value.keys())])
            v_offset = len(p_dict_column_name_value) + 1
            
            # If source is specified (database name, schema, table) then update row based on this columns
            if p_source:
                v_dict_join = {INVERTED_SOURCE_PROPERTIES_SYNONYMS.get('name'):p_source.name, INVERTED_SOURCE_PROPERTIES_SYNONYMS.get('schema'):p_source.schema, INVERTED_SOURCE_PROPERTIES_SYNONYMS.get('table'):p_source.table}    
            
            # If bronze table is specified, then update row for this bronze table
            # For example, needed when drop a bronze table to update exploit
            if p_bronze_table_name:
                v_dict_join = {INVERTED_SOURCE_PROPERTIES_SYNONYMS.get('bronze_table_name'):p_bronze_table_name}
            
            v_join= "AND ".join([f"{column} =:{i+v_offset} " for i, column in enumerate(v_dict_join.keys())])
            v_bindvars = tuple(list(p_dict_column_name_value.values()) + list(v_dict_join.values()))
            v_request = v_request + v_set + " WHERE "+ v_join
            
            # Update last date or creation date (depends on table)
            message = "Updating request : {} Bind Values :  {}, Simulate {}".format(v_request, v_bindvars,p_simulate)
            
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "SET_LASTUPDATE", "START", log_message=message,
                            log_request=v_request)

            if not p_simulate:
                v_cursor.execute(v_request, v_bindvars)
            self.get_db_connection().commit()
            v_cursor.close()
    
            return True
        
        except oracledb.Error as v_err:
            v_error = "ERROR {} with values {}, Simulate {}".format(v_request, v_bindvars,p_simulate)
            
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "UPDATE_EXPLOIT", v_error, log_message='Oracle DB error : {}'.format(str(v_err)))
            
            self.logger.log(pError=v_err, pAction=v_error)
    
            return False
        
        except Exception as v_err:
    
            v_error = "ERROR {} with values {}, Simulate {}".format(v_request, v_bindvars,p_simulate)
    
            if self.verbose:
                self.verbose.log(datetime.now(tz=timezone.utc), "UPDATE_EXPLOIT", v_error, str(v_err))
            
            self.logger.log(pError=v_err, pAction=v_error)
    
            return False


    def __str__(self):
        '''String method'''
        return f"BronzeExploit: exploit_running_loading_table={self.exploit_running_loading_table}, df_param={self.df_exploit_datasource_loading}"


class BronzeDbManager:
    ''' Declare class BronzeDbManager because some methods have arguments of type BronzeDbManager (Circular declaration)'''
    pass

class BronzeDbManager:
    '''Object to manage connection to bronze database
    Provide main functions to manage Bronze DB activities
    '''
    ''' Bronzefcatyre is used to clone table
    Need to generate a new bronze table 
    '''
    bronzefactory = None

    def __init__(self, pBronze_config:BronzeConfig, pLogger:BronzeLogger,p_bronze_exploit:BronzeExploit=None):
        '''Bronze DB manager constructor'''
    
        self.bronzeDb_Manager_config = pBronze_config
        self.bronzeDbManager_env = self.bronzeDb_Manager_config.get_options().environment
        
        # Bronze Database name - defined into config.json file
        self.bronzeDb_Manager_database_name = "BRONZE_" + self.bronzeDbManager_env
        self.bronzeDb_Manager_logger = pLogger
        
        self.pre_proc = self.bronzeDb_Manager_config.get_options().PLSQL_pre_proc
        
        if self.bronzeDb_Manager_config.get_options().PLSQL_pre_proc_args:
            self.pre_proc_args = self.bronzeDb_Manager_config.get_options().PLSQL_pre_proc_args.split(',')
        else:
            self.pre_proc_args = []
        
        self.post_proc = self.bronzeDb_Manager_config.get_options().PLSQL_post_proc
        
        if self.bronzeDb_Manager_config.get_options().PLSQL_post_proc_args:
            self.post_proc_args = self.bronzeDb_Manager_config.get_options().PLSQL_post_proc_args.split(',')
        else:
            self.post_proc_args = []

        self.gather_lh2_bronze_tables_stats_status = False
        self.garbage_options = None
        
        # Establish connection to Bronze schema database
        self.bronzeDb_Manager_Database_param = get_parser_config_settings("database")(self.bronzeDb_Manager_config.get_configuration_file(),
                                                                            self.get_bronze_database_name())
        self.bronzeDb_Manager_db:absdb = DBFACTORY.create_instance(self.bronzeDb_Manager_Database_param.dbwrapper, self.bronzeDb_Manager_config.get_configuration_file())
        self.bronzeDb_Manager_db_connection = self.get_db().create_db_connection(self.bronzeDb_Manager_Database_param)
        self.df_bronze_tables_stats = None

        self.bronze_exploit = p_bronze_exploit

    def __init_garbage_options__(self):
        '''Init garbage option method'''

        self.garbage_options = get_parser_config_settings("garbage_options")(self.bronzeDb_Manager_config.get_configuration_file(), "garbage_options")
        self.gather_lh2_tables_stats_proc = self.garbage_options.PLSQL_gather_lh2_tables_stats_proc
        self.gather_lh2_tables_stats_proc_args = [self.get_db_username()]
        
        if self.garbage_options.PLSQL_gather_lh2_tables_stats_proc_args:
            self.gather_lh2_tables_stats_proc_args = self.gather_lh2_tables_stats_proc_args.append(self.garbage_options.PLSQL_gather_lh2_tables_stats_proc_args.split(','))
        
        self.lh2_tables_tablename = self.garbage_options.LH2_TABLES_TABLENAME
        self.filestorage_for_excel_export = self.garbage_options.filestorage_for_excel_export
    
    def set_bronze_exploit(self,p_bronze_exploit:BronzeExploit):
        ''' Set Bronze Exploit asscociated to BronzeDbManager object'''
        self.bronze_exploit = p_bronze_exploit

    def set_bronzefactory(self,p_bronzefactory):
        '''Set Bronze Factory object '''
        ''' Needed to create instance of BronzeSourceBuilder instance'''
        ''' Used when cloning bronze table'''
        BronzeDbManager.bronzefactory = p_bronzefactory

    def get_bronze_exploit(self):
        ''' Get Bronze_exploit object associated to BronzeDbManager object'''
        return self.bronze_exploit

    def get_environment(self):
        ''' return environment associated to object'''
        return self.bronzeDbManager_env

    def get_bronze_config(self):
        ''' return environment associated to object'''
        return self.bronzeDb_Manager_config

    def get_bronze_logger(self):
        ''' Return logger'''
        return self.bronzeDb_Manager_logger

    def get_db(self)->absdb:
        '''Get DB method'''
        return self.bronzeDb_Manager_db
    

    def get_db_parameters(self):
        '''Get DB parameters method'''
        return self.bronzeDb_Manager_Database_param
    

    def get_db_connection(self):
        '''Get DB connection method'''
        return self.bronzeDb_Manager_db_connection
    

    def get_bronze_database_name(self):
        '''Get bronze DB name method'''
        return self.bronzeDb_Manager_database_name
    

    def get_db_username(self):
        '''Get DB username method'''
        return self.get_db_parameters().p_username
    

    def get_pre_proc(self):
        '''Get pre pro method'''
        return (self.pre_proc, self.pre_proc_args)
    

    def get_post_proc(self):
        '''Get post proc method'''
        return (self.post_proc, self.post_proc_args)
    

    def is_table_exists(self, p_table_name):
        '''Is table exists method'''

        res = False

        if self.get_db_connection():
            res = self.get_db().is_table_exists(p_table_name, self.get_db_username())

        return res
    

    def get_bronze_lastupdated_row(self, p_table_name, p_src_date_criteria, p_where=None):
        '''Get bronze lastupdated row method'''

        last_date = None

        if self.get_db_connection():
            last_date = self.get_db().get_table_max_column(p_table_name, p_src_date_criteria, p_where)

        return last_date
    

    def run_proc(self, p_proc_name='', /, *args,p_verbose=None, p_proc_exe_context='GLOBAL'):
        '''Run proc method'''

        v_start = datetime.now()
        v_return = True
        v_request = "{}({})".format(p_proc_name, str(args))

        try:
            if p_proc_name and self.get_db():
                v_dmbs_output = self.get_db().execute_proc(p_proc_name, *args)
                v_log_message = v_dmbs_output
                
                if self.get_db().last_execute_proc_completion():
                    v_action = "COMPLETED"
                    v_err = None
                    v_return = True
                
                else:
                    v_action = "ERROR"
                    v_err = Exception("Error during execution of procedure {} with {}".format(p_proc_name, args))
                    v_return = False

        except Exception as err:
            v_err = err
            v_action = "ERROR calling Procedure {} with {}".format(p_proc_name, args)
            v_log_message = 'Oracle DB error :{}'.format(str(v_err))
            v_return = False
    
        finally:
            v_duration = datetime.now() - v_start
            self.bronzeDb_Manager_logger.set_logger_properties(p_src_name=self.get_bronze_database_name(), p_src_origin_name=self.get_db_username(), p_src_object_name=p_proc_exe_context, p_request=v_request, p_duration=v_duration)
            
            if p_proc_name and self.get_db():
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "BRONZE_PROC", v_action, log_message=v_log_message)
                
                self.bronzeDb_Manager_logger.log(pError=v_err, pAction=v_action)
    
            return v_return

                 
    def run_pre_proc(self, p_verbose=None):
        '''Run pre proc method'''
        return self.run_proc(self.pre_proc, *self.pre_proc_args, p_verbose=p_verbose, p_proc_exe_context='GLOBAL')
        

    def run_post_proc(self, p_verbose=None):
       '''Run post proc method'''
       return self.run_proc(self.post_proc, *self.post_proc_args, p_verbose=p_verbose, p_proc_exe_context='GLOBAL')
    

    def get_garbage_options(self):
        '''Get garbage options method'''
        return self.garbage_options
    

    def get_gather_lh2_tables_stats_status(self):
        '''Get gather lh2 tables stats status'''
        return self.gather_lh2_bronze_tables_stats_status
    

    def get_lh2_bronze_tables_stats(self):
        '''Get lh2 bronze tables stats'''
        return self.df_bronze_tables_stats
    

    def get_lh2_bronze_buckets(self):
        '''Get lh2 bronze buckets'''
        return self.df_bronze_buckets_parquets
    

    def gather_lh2_bronze_tables_stats(self, p_refresh_stats=True, p_count_rows=False, p_verbose=None):
        '''Run PL/SQL procedure to collect LH2 bronze tables stats for current environment, from Oracle database
        If p_refresh_stats is True, refresh parquets files lists for every external tables 
        (and identify zombies parquets files not associated to any external table)
        If p_count_row, run a select count(1) from table to get number of row. 
        This option could be long
        '''

        def __match_files_to_tables__(p_df, p_list_file_objects):
            '''Function to match files to external tables and handle "zombies"
            p_df is a dataframe mapping LH2_TABLES with same columns - Contains list of external tables 
            p_list_file_objects : list of bucket objects .parquet. Parquets files are linked or not to external tables
            '''      

            v_matches = []
            v_zombies = []
            
            # Dictionary to track file associations
            v_file_associated = {v_file['name']: {'associated':False,'size_mb':v_file['size_mb']} for v_file in p_list_file_objects}

            # Associate files with tables
            for _, v_row in p_df.iterrows():
                v_table_data = v_row.to_dict()
                v_env = v_table_data['ENV']
                v_bucket = v_table_data['BUCKET']
                v_uri_pattern = re.search('o/(.*)', v_table_data['FILE_URI']).group(1)   
                v_matching_files = [v_file for v_file in p_list_file_objects if fnmatch.fnmatch(v_file['name'], v_uri_pattern)]
                
                for v_file in v_matching_files:
                    v_file_associated[v_file['name']]['associated'] = True
                    
                # Update list fo parquets, number of parquet files and total size of parquet files
                v_table_data['LIST_PARQUETS'] = [v_file['name'] for v_file in v_matching_files]
                v_table_data['NUM_PARQUETS'] = len(v_table_data['LIST_PARQUETS'])
                v_table_data['SIZE_MB'] = sum([v_file['size_mb'] for v_file in v_matching_files])
                v_matches.append(v_table_data)
            
            # Add "zombie" files
            v_zombies_files = [v_file_name for v_file_name, v_row in v_file_associated.items() if not v_row['associated']]
            v_zombies_files_size_temp = [v_row['size_mb'] for v_file_name, v_row in v_file_associated.items() if not v_row['associated']]
            v_zombies_files_size = sum(v_zombies_files_size_temp)
            
            if v_zombies_files:
                v_zombies_data = {col: '' for col in p_df.columns}
                
                # Add default values - For ENV and BUCKET set same values of last table inspected
                v_zombies_data['ENV'] = v_env
                v_zombies_data['OWNER'] = ''
                v_zombies_data['TABLE_NAME'] = ZOMBIES_TABLE_NAME
                v_zombies_data['PARTITIONED'] = 'NO'
                v_zombies_data['TABLE_TYPE'] = None
                v_zombies_data['NUM_ROWS'] = 0
                v_zombies_data['SIZE_MB'] = v_zombies_files_size
                v_zombies_data['BUCKET'] = v_bucket
                v_zombies_data['FILE_URI'] = ''
                v_zombies_data['NUM_PARQUETS'] = len(v_zombies_files)
                v_zombies_data['LIST_PARQUETS'] = v_zombies_files
                v_matches.append(v_zombies_data)
            
            return pd.DataFrame(v_matches)
        ''' End function __match_files_to_tables__'''


        v_start = datetime.now()
        v_return = True
        v_request = "Gather Bronze tables stats for {}".format(self.get_bronze_database_name())
        v_log_message = "Starting "+ v_request
        
        if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "GATHER_BRONZE_STATS", "START", log_message=v_log_message)
       
        # Check if options for garbage are set
        if not self.get_garbage_options():
            self.__init_garbage_options__()
    
        try:
            ''' If refresh option Execute PL/SQL Exploit Proc to update list of bronze tables'''
            if p_refresh_stats:
                # Execute PLSQL Proc to gather informations of tables (uri for external tables)
                v_log_message = "Gather Bronze tables stats with PL SQL Proc {0}({1})".format(self.gather_lh2_tables_stats_proc, self.gather_lh2_tables_stats_proc_args)

                if p_verbose:
                        p_verbose.log(datetime.now(tz=timezone.utc),"GATHER_BRONZE_STATS", "START", log_message=v_log_message)

                v_result_run_proc = self.run_proc(self.gather_lh2_tables_stats_proc, *self.gather_lh2_tables_stats_proc_args, p_verbose=p_verbose, p_proc_exe_context='ALL')

                if not v_result_run_proc:
                    raise Exception("ERROR, Executing Procedure {0}({1})".format(self.gather_lh2_tables_stats_proc, self.gather_lh2_tables_stats_proc_args))
            
            '''Execute a SQL query to fetch list of external tables of bronze layer from Exploit tables self.lh2_tables_tablename'''
            v_cursor = self.get_db_connection().cursor()
            v_sql = "select * from " + self.lh2_tables_tablename + " WHERE ENV = \'" + self.bronzeDbManager_env +"\' AND TABLE_TYPE like \'External%\' ORDER BY OWNER,TABLE_NAME"
            v_log_message = "Fetch list of external tables of bronze layer {}".format(v_sql)
            
            if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "GATHER_BRONZE_STATS","RUNNING", log_message=v_log_message)
            
            v_cursor.execute(v_sql)
            v_df_lh2_tables = pd.DataFrame(v_cursor.fetchall())
            v_df_lh2_tables.columns = [x[0] for x in v_cursor.description]
            v_cursor.close()

            if p_count_rows:
                '''Count rows for each tables'''


                v_message = "Count rows for  list of external tables of bronze layer {}".format(self.bronzeDbManager_env)
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc),"GATHER_BRONZE_STATS","RUNNING",log_message=v_message)

                for v_index, v_row in v_df_lh2_tables.iterrows():
                    v_table_data = v_row.to_dict()
                    v_table_name = v_table_data['TABLE_NAME']
                    v_num_rows = self.get_db().get_num_rows(v_table_name)

                    #v_num_rows = 0

                    v_df_lh2_tables.at[v_index,'NUM_ROWS'] = v_num_rows
                '''end of count rows'''

            '''
                Get buckets used for bronze layers
                Get distinct bucket names from the column
            '''
            v_bronze_buckets_array = v_df_lh2_tables['BUCKET'].unique()

            '''Create a new DataFrame from the distinct values'''
            self.df_bronze_buckets_parquets = pd.DataFrame(v_bronze_buckets_array, columns=['BUCKET'])

            '''Add a column 'LIST_PARQUETS' with default values (here, empty lists)'''
            self.df_bronze_buckets_parquets['BUCKET_LIST_PARQUETS'] = [[] for _ in range(len(self.df_bronze_buckets_parquets))]

            v_log_message = "Buckets used into bronze layer {}".format(str(self.df_bronze_buckets_parquets))

            if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc),"GATHER_BRONZE_STATS", "RUNNING", log_message=v_log_message)

            v_bronze_bucket_proxy = BronzeBucketProxy(self.bronzeDbManager_env, self.bronzeDb_Manager_config)

            '''Iterate over the 'BUCKET' column and change the value of 'LIST_PARQUETS' with list of objects name into each bucket'''
            for v_index, v_row in self.df_bronze_buckets_parquets.iterrows():
                v_bucket_name = v_row['BUCKET']
                v_bronze_bucket_proxy.set_bucket_by_name(v_bucket_name)
                v_bucket:AbsBucket = v_bronze_bucket_proxy.get_bucket()
                v_bucket_list_objects = v_bucket.list_objects()
                self.df_bronze_buckets_parquets.at[v_index, 'BUCKET_LIST_PARQUETS'] = [{'name':o.name,'size_mb':int(o.size or 0)/1024} for o in v_bucket_list_objects]

            v_log_message = "Bucket files inventory done {}".format(str(self.df_bronze_buckets_parquets))

            if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "GATHER_BRONZE_STATS", "RUNNING", log_message=v_log_message)

            '''
            Iterate Buckets and check for each external tables associated to the bucket, which bucket files are associated to the table
            Bucket files not associated to any table, will associated to a "zombies" table    
            '''
            v_df_updated_tables_stats = pd.DataFrame()

            for v_index,v_row in self.df_bronze_buckets_parquets.iterrows():
                v_bucket_name = v_row['BUCKET']
                v_df_bucket_tables = v_df_lh2_tables[v_df_lh2_tables['BUCKET'] == v_bucket_name]
                v_log_message = "Check which files of bucket {0} are associated to external tables.".format(v_bucket_name)

                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "GATHER_BRONZE_STATS", "RUNNING", log_message=v_log_message)

                v_df_updated_bucket_tables = __match_files_to_tables__(v_df_bucket_tables, v_row['BUCKET_LIST_PARQUETS'])
                v_df_updated_tables_stats = pd.concat([v_df_updated_tables_stats, v_df_updated_bucket_tables], ignore_index=True)

            self.df_bronze_tables_stats = v_df_updated_tables_stats.fillna(0)
            self.gather_lh2_bronze_tables_stats_status = True
            v_log_message = "Files are associated to external tables"

            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "GATHER_BRONZE_STATS" ,"END", log_message=v_log_message)

            '''End of gather process    '''
            v_log_message = "COMPLETED - " + v_request
            v_action = "COMPLETED"
            v_err = None
            v_return = True

        except Exception as err:
            v_err = err
            v_action = "ERROR - " + v_request
            v_log_message = str(v_err)
            v_return = False

        finally:
            v_duration = datetime.now() - v_start
            self.bronzeDb_Manager_logger.set_logger_properties(p_src_name=self.get_bronze_database_name(), p_src_origin_name=self.get_db_username(), p_src_object_name="ALL", p_request=v_request, p_duration=v_duration)
            
            if self.get_db():
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "GATHER_BRONZE_STATS", v_action, log_message=v_log_message)
    
                self.bronzeDb_Manager_logger.log(pError=v_err, pAction=v_action)

            return v_return
    

    def update_lh2_bronze_tables_stats(self, p_verbose=None):
        '''Update databse LH2_tables with updated stats'''

        if not self.get_gather_lh2_tables_stats_status():
            v_log_message = "Need to refresh stats before update LH2 Bronze table stats "
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "UPDATE_BRONZE_STATS", "START", log_message=v_log_message)
            
            return False
    
        v_start = datetime.now()
        v_return = True
        v_request = "Update external tables stats of bronze layer {} into table {}:".format(self.bronzeDbManager_env, self.lh2_tables_tablename)
        
        try:
            v_log_message = "Starting "+v_request
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc),"UPDATE_BRONZE_STATS", "START", log_message=v_log_message)
            
            v_cursor = self.get_db_connection().cursor()
            ''' Filtering tables to exclude Zombie_table_name with listf of zombies parquet files'''
            v_filtered_df_lh2_bronze_tables = create_filter_mask(self.get_lh2_bronze_tables_stats(),f'TABLE_NAME != \'{ZOMBIES_TABLE_NAME}\'')
            
            for v_index, v_row in v_filtered_df_lh2_bronze_tables.iterrows():
                v_rown_list_parquets = list_to_string(v_row['LIST_PARQUETS'])
                v_sql = "UPDATE " + self.lh2_tables_tablename + " SET NUM_ROWS = :1, SIZE_MB = :2, NUM_PARQUETS = :3, LIST_PARQUETS = :4 WHERE OWNER = :5 AND TABLE_NAME = :6"
                v_bindvars = (int(v_row['NUM_ROWS'] or 0), int(v_row['SIZE_MB'] or 0), int(v_row['NUM_PARQUETS'] or 0), v_rown_list_parquets, v_row['OWNER'], v_row['TABLE_NAME'])
                
                # v_log_message = "Updating {0}.{1}, num_rows {2}, size_mb {3}, num_parquets {4}\n Request : {5}".format(v_row['OWNER'], v_row['TABLE_NAME'], int(v_row['NUM_ROWS'] or 0), int(v_row['SIZE_MB'] or 0), int(v_row['NUM_PARQUETS'] or 0),v_sql)
                
                # if p_verbose:
                #     p_verbose.log(datetime.now(tz=timezone.utc), "UPDATE_BRONZE_STATS", "RUNNING", log_message=v_log_message)

                v_cursor.execute(v_sql,v_bindvars )

            self.get_db_connection().commit()
            v_cursor.close()
           
            v_log_message = "COMPLETED - " + v_request
            v_action = "COMPLETED"
            v_err = None
            v_return = True
            
        except Exception as err:
            v_err = err
            v_action = "ERROR - Update external tables stats of bronze layer {} into table {}:".format(self.bronzeDbManager_env, self.lh2_tables_tablename)
            v_log_message = str(v_err)
            v_return = False

        finally:
            v_duration = datetime.now() - v_start
            self.bronzeDb_Manager_logger.set_logger_properties(p_src_name=self.get_bronze_database_name(), p_src_origin_name=self.get_db_username(), p_src_object_name="ALL", p_request=v_request, p_duration=v_duration)
            
            if self.get_db():
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "UPDATE_TABLE_BRONZE_STATS", v_action, log_message=v_log_message)
                self.bronzeDb_Manager_logger.log(pError=v_err, pAction=v_action)

            return v_return
        

    def to_excel_lh2_bronze_tables_stats(self, p_excel_filename="lh2_bronze_tables_stats.xlsx", p_verbose=None):
        '''Export LH2 bronze tables stats to Excel file'''
        
        if not self.get_gather_lh2_tables_stats_status():
            v_log_message = "Need to refresh stats before laucnh garbage collector "
    
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "EXPORT_BRONZE_STATS", "START", log_message=v_log_message)
    
            return None

        v_start = datetime.now()
        v_return = None
        v_excel_file_tmp = p_excel_filename
        v_request = "Exporting to temporay Excel file {} :".format(v_excel_file_tmp)

        try:
            '''Create access to filestorage where to export excel file'''
            v_filestorage_param = get_parser_config_settings("filestorage")(self.bronzeDb_Manager_config.get_configuration_file(), self.filestorage_for_excel_export)
            v_filestorage = FILESTORAGEFACTORY.create_instance(v_filestorage_param.filestorage_wrapper, **v_filestorage_param._asdict())
           
            '''Export dataframe to local Excel file'''
            v_excel_file_tmp = os.path.join(self.bronzeDb_Manager_config.get_tempdir(), 'tmp_' + p_excel_filename)
            v_log_message = v_request
    
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "EXPORT_BRONZE_STATS", "START", log_message=v_log_message)

            '''
                Converts all list elements in the DataFrame to strings with default delimiter , 
                used for list of parquets files
            '''
            v_df_converted_lists_to_strings = df_convert_lists_to_strings(self.get_lh2_bronze_tables_stats())

            ''' Export Dataframe to local excel file'''
            v_df_converted_lists_to_strings.to_excel(v_excel_file_tmp, index=False)
            
            '''Copy generated Excel to filestorage'''
            v_log_message = "Copy temporay Excel file {} -> {}:".format(v_excel_file_tmp, v_filestorage.get_filestorage_name())
    
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "EXPORT_BRONZE_STATS", "RUNNING", log_message=v_log_message)
        
            v_destination_excel_file = v_filestorage.put_file(p_excel_filename, v_excel_file_tmp)
            os.remove(v_excel_file_tmp)
            
            v_request = "Export Excel file {} :".format(v_destination_excel_file)
            v_log_message = "COMPLETED - " + v_request
            v_action = "COMPLETED"
            v_err = None
            v_return = v_destination_excel_file
            
        except Exception as err:
            v_err = err
            v_action = "ERROR  - " + v_request
            v_log_message = str(v_err)
            v_return = None

        finally:
            v_duration = datetime.now() - v_start
            self.bronzeDb_Manager_logger.set_logger_properties(p_src_name=self.get_bronze_database_name(), p_src_origin_name=self.get_db_username(), p_src_object_name="ALL", p_request=v_request, p_duration=v_duration)
            
            if self.get_db():
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "EXPORT_BRONZE_STATS", v_action, log_message=v_log_message)
    
                self.bronzeDb_Manager_logger.log(pError=v_err, pAction=v_action)

            return v_return
        

    def __delete_parquet_files__(self, p_bucket_name, p_parquet_list, p_simulate=False, p_verbose=None):
        '''Into bucket, Drop list of parquets files'''

        v_start = datetime.now()
        v_return = False
        v_file = ''
        v_request = "Into bucket {}, Simulate {} \n Deleting {} parquet files : {}".format(p_bucket_name, str(p_simulate), len(p_parquet_list), p_parquet_list)
        
        try:
            v_log_message = v_request
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "DELETE_PARQUETS", "START", log_message=v_log_message)
            
            v_bronze_bucket_proxy = BronzeBucketProxy(self.bronzeDbManager_env, self.bronzeDb_Manager_config)
            v_bronze_bucket_proxy.set_bucket_by_name(p_bucket_name)
            v_bucket:AbsBucket = v_bronze_bucket_proxy.get_bucket()
            
            # Iterate parquet list and delete object into bucket
            for v_object in p_parquet_list:
                v_file = v_object
                
                if not p_simulate:
                    v_bucket.delete_object(v_object)
            
            v_log_message = "COMLPLETED - "+ v_request
            v_action = "COMPLETED" if not p_simulate else "COMPLETED - SIMULATE"
            v_err = None
            v_return = True

        except Exception as err:
            v_err = err
            v_action = "ERROR - Into bucket {}, can not drop parquet file {}".format(p_bucket_name, v_file)
            v_log_message = str(v_err)
            v_return = False

        finally:
            v_duration = datetime.now() - v_start
            self.bronzeDb_Manager_logger.set_logger_properties(p_src_name=self.get_bronze_database_name(), p_src_origin_name=self.get_db_username(), p_src_object_name=p_bucket_name, p_request=v_request, p_duration=v_duration)
            
            if self.get_db():
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "DELETE_PARQUETS", v_action, log_message=v_log_message)

                self.bronzeDb_Manager_logger.log(pError=v_err, pAction=v_action)

            return v_return

    
    def bronze_garbage_collector(self,p_simulate=False,p_verbose=None):
        '''Garbage collector to delete parquet files not associated to any external tables (zombies parquet files)'''

        v_start = datetime.now()
        v_return = False
        v_request = "Garbage collector for {} - Simulate {}".format(self.bronzeDbManager_env, str(p_simulate))
        
        if not self.get_gather_lh2_tables_stats_status():
            v_log_message = "Need to refresh stats before laucnh garbage collector "
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "GARBAGE_COLLECTOR", "START", log_message=v_log_message)

                return False

        try:
            v_log_message = "Starting "+ v_request
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "GARBAGE_COLLECTOR", "START", log_message=v_log_message)
            
            # Filter on ZOMBIES TABLE
            v_mask = self.df_bronze_tables_stats['TABLE_NAME'] == ZOMBIES_TABLE_NAME
            
            for v_index, v_row in self.df_bronze_tables_stats[v_mask].iterrows():
                v_bucket_name = v_row['BUCKET']
                v_parquet_list = v_row['LIST_PARQUETS']
                v_result = self.__delete_parquet_files__(p_bucket_name=v_bucket_name, p_parquet_list=v_parquet_list, p_simulate=p_simulate, p_verbose=p_verbose)
                
                if not v_result:
                    raise Exception("ERROR  Deleting parquet files into bucket {}".format(v_bucket_name))
                
            v_log_message = "COMPLETED - " + v_request
            v_action = "COMPLETED" if not p_simulate else "COMPLETED - SIMULATE"
            v_err = None
            v_return = True
            
        except Exception as err:
            v_err = err
            v_action = "ERROR - " + v_request
            v_log_message = str(v_err)
            v_return = False

        finally:
            v_duration = datetime.now() - v_start
            self.bronzeDb_Manager_logger.set_logger_properties(p_src_name=self.get_bronze_database_name(), p_src_origin_name=self.get_db_username(), p_src_object_name="ALL", p_request=v_request, p_duration=v_duration)
            
            if self.get_db():
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "GARBAGE_COLLECTOR", v_action, log_message=v_log_message)
                
                self.bronzeDb_Manager_logger.log(pError=v_err, pAction=v_action)
    
            return v_return

    def bronze_drop_table(self, p_table_name, p_simulate=False, p_verbose=None):
        '''Drop bronze table : drop external table into database and delete associated parquet files'''

        v_start = datetime.now()
        v_return = False
        v_request = "Drop table {}.{} and associated parquet files - Simulate {}".format(self.get_db_username(),
                                                                                         p_table_name, str(p_simulate))

        if not self.get_gather_lh2_tables_stats_status():
            v_log_message = "Need to refresh stats before dropping table {}.{} ".format(self.get_db_username(),
                                                                                        p_table_name)

            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLE", "START", log_message=v_log_message)

            return False

        try:
            v_log_message = "Starting {} - Simulate : {}".format(v_request, str(p_simulate))

            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLE", "START", log_message=v_log_message)

            if not self.bronze_exploit:
                raise Exception("Exploit object not associated to BronzeDbManager Object")

            # Test if table exists
            if not self.is_table_exists(p_table_name):
                v_log_message = "Table {}.{} does not exist ".format(self.get_db_username(), p_table_name)

                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLE", "START", log_message=v_log_message)

                v_log_message = "COMPLETED - " + v_request
                v_action = "COMPLETED" if not p_simulate else "COMPLETED - SIMULATE"
                v_err = None
                v_return = True

                return True

            ''' Filter on table name  and ower = db user name '''
            v_mask = (self.df_bronze_tables_stats['OWNER'] == self.get_db_username()) & (
                        self.df_bronze_tables_stats['TABLE_NAME'] == p_table_name)

            for v_index, v_row in self.df_bronze_tables_stats[v_mask].iterrows():
                v_table_name = v_row['TABLE_NAME']
                ''' Drop table into database '''
                if not p_simulate:
                    v_result_run_proc = self.run_proc('LH2_BRONZE_ADMIN_PKG.DROP_TABLE_PROC', *[v_table_name,
                                                                                                self.bronzeDb_Manager_config.get_options().bronze_bis_suffixe],
                                                      p_verbose=p_verbose, p_proc_exe_context=v_table_name)
                else:
                    v_result_run_proc = True

                if not v_result_run_proc:
                    raise Exception("ERROR dropping table {}.{}".format(self.get_db_username(), v_table_name))

                ''' delete associated parquets files '''
                v_bucket_name = v_row['BUCKET']
                v_parquet_list = v_row['LIST_PARQUETS']
                v_result = self.__delete_parquet_files__(p_bucket_name=v_bucket_name, p_parquet_list=v_parquet_list,
                                                         p_simulate=p_simulate, p_verbose=p_verbose)

                if not v_result:
                    raise Exception("ERROR  Deleting parquet files into bucket {}".format(v_bucket_name))

                if v_result:
                    ''' 
                    If drop table and parquet files successfully,
                    set BRONZE_LASTUPLOADED_PARQUET to null,
                    set SRC_DATE_LASTUPDATE to RESET_DATE_LASTUPDATE for Incremental table (Partitioned table)
                    set bronze_status = DROPPED
                    '''
                    v_dict_update_exploit = dict()
                    v_dict_update_exploit['bronze_lastuploaded_parquet'] = None
                    v_row_exploit = self.bronze_exploit[v_table_name]
                    v_reset_date_lastupdate = v_row_exploit.reset_lastupdate
                    v_dict_update_exploit['last_update'] = v_reset_date_lastupdate
                    v_dict_update_exploit['bronze_status'] = DICT_STATUS_CODE['dropped']
                    v_dict_update_exploit['bronze_update'] = datetime.now(tz=timezone.utc)

                    if p_verbose:
                        v_log_message = "Update Exploit loading table {} , reset {} - Simulate = {}".format(
                            v_table_name, v_dict_update_exploit, str(p_simulate))
                        p_verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLES_QUERY", "RUNNING",
                                      log_message=v_log_message)

                    if not p_simulate:
                        if not self.bronze_exploit.update_exploit(v_dict_update_exploit, p_bronze_table_name=v_table_name):
                            raise Exception(
                                "ERROR - Update Exploit table {} : {}".format(v_table_name, v_dict_update_exploit))

            v_log_message = "COMPLETED - " + v_request
            v_action = "COMPLETED" if not p_simulate else "COMPLETED - SIMULATE"
            v_err = None
            v_return = True

        except Exception as err:
            v_err = err
            v_action = "ERROR - Drop table {}.{} and associated parquet files".format(self.get_db_username(),
                                                                                      p_table_name)
            v_log_message = str(v_err)
            v_return = False

        finally:
            v_duration = datetime.now() - v_start
            self.bronzeDb_Manager_logger.set_logger_properties(p_src_name=self.get_bronze_database_name(),
                                                               p_src_origin_name=self.get_db_username(),
                                                               p_src_object_name=p_table_name, p_request=v_request,
                                                               p_duration=v_duration)

            if self.get_db():
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "DROP_EXTERNAL_TABLE", v_action,
                                  log_message=v_log_message)
                self.bronzeDb_Manager_logger.log(pError=v_err, pAction=v_action)

            return v_return

    def bronze_clone_table(self, p_table_name, p_bronze_db_manager_dest:BronzeDbManager, p_simulate=False, p_verbose=None):
        '''Clone bronze table : drop external table into database and delete associated parquet files'''

        v_start = datetime.now()
        v_return = False
        v_request = "Clone table {}.{} and associated parquet files - Simulate {}".format(self.get_db_username(),
                                                                                         p_table_name, str(p_simulate))

        if not self.get_gather_lh2_tables_stats_status():
            v_log_message = "Need to refresh stats before cloning table {}.{}; Need to update list of parquet files ".format(self.get_db_username(),
                                                                                        p_table_name)
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "CLONE_TABLE", "START", log_message=v_log_message)
            return False

        try:
            v_log_message = "Starting {} - Simulate : {}".format(v_request, str(p_simulate))

            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "CLONE_TABLE", "START", log_message=v_log_message)

            # Test if table exists
            if not self.is_table_exists(p_table_name):
                v_log_message = "Table {}.{} does not exist ".format(self.get_db_username(), p_table_name)
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLE", "START", log_message=v_log_message)

                v_log_message = "COMPLETED - " + v_request
                v_action = "COMPLETED" if not p_simulate else "COMPLETED - SIMULATE"
                v_err = None
                v_return = True

                return True

            ''' Filter on table name  and ower = db user name '''
            v_mask = (self.df_bronze_tables_stats['OWNER'] == self.get_db_username()) & (
                        self.df_bronze_tables_stats['TABLE_NAME'] == p_table_name)

            v_bronze_config_dest = p_bronze_db_manager_dest.get_bronze_config()
            v_bronze_exploit_dest = p_bronze_db_manager_dest.get_bronze_exploit()
            v_bronze_logger_dest = p_bronze_db_manager_dest.get_bronze_logger()
            v_bronze_exploit_src = self.get_bronze_exploit()

            ''' Test if Bronzefactory object is not already instanciated ( global class variable) '''
            if not BronzeDbManager.bronzefactory:
                raise Exception("ERROR Bronze Factory object is not associated to current BronzeDbManager object {}".format(self.get_bronze_database_name()))

            for v_index, v_row in self.df_bronze_tables_stats[v_mask].iterrows():
                v_table_name = v_row['TABLE_NAME']
                ''' Drop bronze destination table'''
                v_drop_result = p_bronze_db_manager_dest.bronze_drop_table(p_table_name=v_table_name,p_simulate=p_simulate,p_verbose=p_verbose)

                if not v_drop_result:
                    raise Exception("ERROR dropping bronze table {}.{}".format(p_bronze_db_manager_dest.get_db_username(), v_table_name))

                ''' Generate destination bronze tables by copying parquet list '''
                '''
                    Get source tables information from destination exploit datasource loading table
                    Create Bronze_builder associated to the destination table (Table source information must exist into exploit datasource loading table
                '''
                v_bronze_exploit_dest = p_bronze_db_manager_dest.get_bronze_exploit()
                v_datasource_loading = v_bronze_exploit_dest[v_table_name]
                v_bronze_builder = BronzeDbManager.bronzefactory.create_instance(v_datasource_loading.type, v_datasource_loading, v_bronze_config_dest, p_bronze_db_manager_dest, v_bronze_logger_dest)

                '''Get Date_Lastupdated from Exploir Source row'''
                ''' to update destination Exploit with same value : updated by BronzeGenerator.generate_by_cloning_parquets'''
                v_exploit_src_row = v_bronze_exploit_src[p_table_name]
                v_date_lastupdate = v_exploit_src_row.last_update
                v_bronze_builder.set_bronze_date_lastupdate(v_date_lastupdate)

                ''' Create Generator to build destination bronze table based'''
                v_bronze_plant_dest = BronzeGenerator(v_bronze_builder, v_bronze_exploit_dest, v_bronze_logger_dest)
                ''' 
                    Create Bronze Generator
                    Generate Bronze table by cloning parquets files
                '''
                ''' Get list of parquet files to clone from source'''
                v_parquet_list_src = v_row['LIST_PARQUETS']
                ''' Create connection to source bucket'''
                v_bucket_name_src = v_row['BUCKET']
                v_bronze_bucket_proxy_src = BronzeBucketProxy(self.get_environment(), self.bronzeDb_Manager_config)
                v_bronze_bucket_proxy_src.set_bucket_by_name(v_bucket_name_src)
                v_bucket_src: AbsBucket = v_bronze_bucket_proxy_src.get_bucket()

                ''' Generate Bronze table into destination '''
                v_result = v_bronze_plant_dest.generate_by_cloning_parquets(p_bucket_src=v_bucket_src, p_parquet_list_src=v_parquet_list_src,
                                                         p_simulate=p_simulate, p_verbose=p_verbose)

                if not v_result:
                    raise Exception("ERROR  Cloning table {} to {}".format(v_table_name,p_bronze_db_manager_dest.get_db_username()))


            v_log_message = "COMPLETED - " + v_request
            v_action = "COMPLETED" if not p_simulate else "COMPLETED - SIMULATE"
            v_err = None
            v_return = True

        except Exception as err:
            v_err = err
            v_action = "ERROR - Cloning table {}.{} and associated parquet files".format(self.get_db_username(),
                                                                                      p_table_name)
            v_log_message = str(v_err)
            v_return = False

        finally:
            v_duration = datetime.now() - v_start
            self.bronzeDb_Manager_logger.set_logger_properties(p_src_name=self.get_bronze_database_name(),
                                                               p_src_origin_name=self.get_db_username(),
                                                               p_src_object_name=p_table_name, p_request=v_request,
                                                               p_duration=v_duration)

            if self.get_db():
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "CLONE_EXTERANL_TABLE", v_action,
                                  log_message=v_log_message)
                self.bronzeDb_Manager_logger.log(pError=v_err, pAction=v_action)

            return v_return

    def __get_df_bronze_tables_by_query__(self, p_query: str, p_verbose=None):
        ''' get list of bronze tables
        list of tables return by query on dataframe df_lh2_tables_stats
        Does not include 'zombies'
        '''

        if not self.get_gather_lh2_tables_stats_status():
            v_log_message = "Need to refresh stats before dropping tables with query {} ".format(p_query)

            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLES_QUERY", "START", log_message=v_log_message)

            return None

        v_filtered_df_lh2_bronze_table_without_zombies = create_filter_mask(self.get_lh2_bronze_tables_stats(),
                                                                            f'TABLE_NAME != \'{ZOMBIES_TABLE_NAME}\'')
        v_filtered_df_lh2_bronze_tables = create_filter_mask(v_filtered_df_lh2_bronze_table_without_zombies,
                                                             p_query)

        print(v_filtered_df_lh2_bronze_tables)
        if v_filtered_df_lh2_bronze_tables is None:
            raise Exception("ERROR, Filtering bronze tables list, review your drop query")

        return v_filtered_df_lh2_bronze_tables

    def bronze_drop_tables_by_query(self, p_query:str, p_simulate=False, p_ask_to_drop=True, p_verbose=None):
        '''drop bronze tables 
        list of tables return by query on dataframe df_lh2_tables_stats
        '''

        v_start = datetime.now()
        v_return = False
        v_request = "Drop tables on query {} - Simulate {}".format(p_query, str(p_simulate))
        v_table_list_to_drop = None

        try:
            v_log_message = "Starting " + v_request
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLES_QUERY", "START", log_message=v_log_message)

            if not self.bronze_exploit:
                raise Exception("Exploit object not associated to BronzeDbManager Object")

            v_filtered_df_lh2_bronze_tables = self.__get_df_bronze_tables_by_query__(p_query=p_query,p_verbose=p_verbose)
            v_table_list_to_drop = v_filtered_df_lh2_bronze_tables['TABLE_NAME'].tolist()

            if p_verbose:
                v_log_message = "{} tables to be dropped : {}".format(len(v_table_list_to_drop), v_table_list_to_drop)
                p_verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLES_QUERY", "RUNNING", log_message=v_log_message)
            
            if p_ask_to_drop:
                v_user_ask_to_drop = ask_user("Confirm to drop tables")
                
                if v_user_ask_to_drop == "N":
                    v_request = "Drop tables on query {} : \n{}".format(p_query, v_table_list_to_drop)
                    v_log_message = "COMPLETED - NOT CONFIRMED" + v_request
                    v_action = "COMPLETED" if not p_simulate else "COMPLETED - SIMULATE"
                    v_err = None
                    v_return = True

                    return True

            for _,v_row in v_filtered_df_lh2_bronze_tables.iterrows():
                v_table_data = v_row.to_dict()
                v_table_name = v_table_data['TABLE_NAME']
                ''' Drop table '''
                v_drop_result = self.bronze_drop_table(p_table_name=v_table_name,p_simulate=p_simulate, p_verbose=p_verbose)
                
            v_request = "Drop tables on query {} : \n{}".format(p_query, v_table_list_to_drop)
            v_log_message = "COMPLETED - " + v_request
            v_action = "COMPLETED" if not p_simulate else "COMPLETED - SIMULATE"
            v_err = None
            v_return = True
            
        except Exception as err:
            v_err = err
            v_action = "ERROR - Drop table on query {} : \n{}".format(p_query, v_table_list_to_drop)
            v_log_message = str(v_err)
            v_return = False

        finally:
            v_duration = datetime.now() - v_start
            self.bronzeDb_Manager_logger.set_logger_properties(p_src_name=self.get_bronze_database_name(), p_src_origin_name=self.get_db_username(), p_src_object_name="ALL", p_request=v_request, p_duration=v_duration)
            
            if self.get_db():
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLES_QUERY", v_action, log_message=v_log_message)
                self.bronzeDb_Manager_logger.log(pError=v_err, pAction=v_action)

            return v_return

    def bronze_clone_tables_parquets_by_query(self, p_query: str, p_bronzedb_manager_dest : BronzeDbManager, p_simulate=False,
                                              p_ask_to_clone=True, p_verbose=None):
        '''Clone bronze tables
        list of tables return by query on dataframe df_lh2_tables_stats
        '''

        v_start = datetime.now()
        v_return = False
        v_request = "Clone tables parquets on query {} - Simulate {}".format(p_query, str(p_simulate))
        v_table_list_to_clone = None

        try:
            v_log_message = "Starting " + v_request

            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "CLONE_TABLES_QUERY", "START", log_message=v_log_message)

            v_filtered_df_lh2_bronze_tables = self.__get_df_bronze_tables_by_query__(p_query=p_query,
                                                                                     p_verbose=p_verbose)
            v_table_list_to_clone = v_filtered_df_lh2_bronze_tables['TABLE_NAME'].tolist()

            if p_verbose:
                v_log_message = "{} tables to be cloned : {}".format(len(v_table_list_to_clone), v_table_list_to_clone)
                p_verbose.log(datetime.now(tz=timezone.utc), "CLONE_TABLES_QUERY", "RUNNING", log_message=v_log_message)

            if p_ask_to_clone:
                v_user_ask_to_clone = ask_user("Confirm to clone tables")

                if v_user_ask_to_clone == "N":
                    v_request = "Clone tables on query {} : \n{}".format(p_query, v_table_list_to_clone)
                    v_log_message = "COMPLETED - NOT CONFIRMED" + v_request
                    v_action = "COMPLETED" if not p_simulate else "COMPLETED - SIMULATE"
                    v_err = None
                    v_return = True
                    return True

            for _, v_row in v_filtered_df_lh2_bronze_tables.iterrows():
                v_table_data = v_row.to_dict()
                v_table_name = v_table_data['TABLE_NAME']
                ''' Clone bronze table to destination '''
                v_clone_result= self.bronze_clone_table(p_table_name=v_table_name,p_bronze_db_manager_dest=p_bronzedb_manager_dest,p_simulate=p_simulate,p_verbose=p_verbose)

            v_request = "Clone tables on query {} : \n{}".format(p_query, v_table_list_to_clone)
            v_log_message = "COMPLETED - " + v_request
            v_action = "COMPLETED" if not p_simulate else "COMPLETED - SIMULATE"
            v_err = None
            v_return = True

        except Exception as err:
            v_err = err
            v_action = "ERROR - Cloning tables on query {} : \n{}".format(p_query, v_table_list_to_clone)
            v_log_message = str(v_err)
            v_return = False

        finally:
            v_duration = datetime.now() - v_start
            self.bronzeDb_Manager_logger.set_logger_properties(p_src_name=self.get_bronze_database_name(),
                                                               p_src_origin_name=self.get_db_username(),
                                                               p_src_object_name="ALL", p_request=v_request,
                                                               p_duration=v_duration)

            if self.get_db():
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLES_QUERY", v_action,
                                  log_message=v_log_message)
                self.bronzeDb_Manager_logger.log(pError=v_err, pAction=v_action)

            return v_return


class BronzeBucketProxy:
    '''Class to provide a substitute to manage bronze bucket.
    add actions to get settings informations to connect to bronze buckets (DEBUG, DEV, STG, PRD)
    create OCIBUcket object to connect on
    '''

    def __init__(self, p_env, p_bronze_config:BronzeConfig):
        '''BronzeBucketProxy constructor'''

        self.env = p_env 
        self.bronze_config = p_bronze_config
        
        if self.env == "DEBUG":
            self.debug = True
            self.oci_settings = get_parser_config_settings("filestorage")(self.bronze_config.get_configuration_file(), "BRONZE_BUCKET_DEBUG")
            self.bucket_prefix = self.bronze_config.get_oci_settings().storage_name
        else:
            self.debug = False
            self.oci_settings = get_parser_config_settings("filestorage")(self.bronze_config.get_configuration_file(), "BRONZE_BUCKET")
            self.bucket_prefix = self.bronze_config.get_oci_settings().storage_name + "-" + self.env


        self.bucket_extension = None
        self.bronze_bucket_settings = None
        self.bucket = None
            

    def set_bucket_by_extension(self, p_bucket_extension):
        '''Define bucket name'''

        v_bucket_name = None
        
        if not self.debug:
            v_bucket_name = self.bucket_prefix + "-" + p_bucket_extension
        else:
            v_bucket_name = self.bucket_prefix

        self.bucket_extension = p_bucket_extension
        self.set_bucket_by_name(v_bucket_name)
            

    def set_bucket_by_name(self, p_bucket_name):
        self.bronze_bucket_settings=self.bronze_config.get_oci_settings()._replace(storage_name=p_bucket_name)

        # else:
        #     self.bronze_bucket_settings = type(self.bronze_config.get_oci_settings())._make(self.bronze_config.get_oci_settings())
        
        # Initialize bucket when set a new storage name
        self.bucket = None
            
    def connect(self):
        '''Connect method'''

        if self.get_bronze_bucket_settings():
            self.bucket = FILESTORAGEFACTORY.create_instance(self.get_bronze_bucket_settings().filestorage_wrapper, **self.get_bronze_bucket_settings()._asdict())
        else:
            self.bucket = None
    
        return self.bucket
    
    def get_bucket(self):
        '''Get bucket method'''
        if not self.bucket:
            self.connect()

        return self.bucket
    
    def get_bronze_bucket_settings(self):
        '''Get Bronze bucket settings method'''
        return self.bronze_bucket_settings
    
    def get_bucket_name(self):
        '''Get bucket name method'''
        if self.get_bronze_bucket_settings():
            return self.bronze_bucket_settings.storage_name
        else:
            return None
    
    def get_oci_objectstorage_url(self):
        '''Get OCI objectstorage URL'''
        if self.get_bronze_bucket_settings():
            return self.bronze_bucket_settings.url
        else:
            return None
    
    def get_oci_adw_credential(self):
        '''Get OCI adw credential'''
        if self.get_bronze_bucket_settings():
            return self.bronze_bucket_settings.setting2
        else:
            return None
        
class BronzeSourceBuilder:
    '''BronzeSourceBuilder class'''

    # ENV : environement DEV, STG, PRD
    # config_file : dictionary json config file with connection parameters
    # SRC_TYPE	Type of source	DB SRC_NAME	Database short name
    # EBS SRC_ORIGIN_NAME	Database schema name	exemple :
    # APPS SRC_OBJECT_NAME	Table name	example GL_PERIODS
    # SRC_OBJECT_CONSTRAINT	where clause for filtering data
    # SRC_FLAG_INCR : integrate date in incremental mode, based on date lastupdate
    # SRC_date_where : clause where to filter on records date, might depend on table
    # SRC_date_lastupdate : update last timestamp for data integration
    # FORCE_ENCODE : if UnicodeError during fetch, custom select by converting column of type VARCHAR to a specific CHARSET

    def __init__(self, p_source_properties:SourceProperties, p_bronze_config:BronzeConfig, p_bronzeDb_Manager:BronzeDbManager, p_logger:BronzeLogger):
        '''BronzeSourceBuilder constructor'''
        self.bronze_source_properties = p_source_properties
        self.bronze_config = p_bronze_config
        self.bronzedb_manager = p_bronzeDb_Manager
        self.env = self.bronze_config.get_options().environment

        # Create "tmp" folder to save parquet files
        self.__set_local_workgingdir__(self.bronze_config.get_tempdir())

        self.df_table_content = pd.DataFrame()
       
        # List of parquet file generated
        self.parquet_file_list = []  # format list : [{"file_name":parquet_file_name1,"source_file":source_file1},{"file_name":parquet_file_name2,"source_file":source_file2},...]
        
        # List of parquet file to send
        self.parquet_file_list_tosend = [] # # format list : [{"file_name":parquet_file_name1,"source_file":source_file1},{"file_name":parquet_file_name2,"source_file":source_file2},...]
        
        # Sent parquet files to OCI bucket
        self.bucket_list_parquet_files_sent = []
        
        # Format the timestamp as a string with a specific format (Year-Month-Day)
        self.today = datetime.now(tz=timezone.utc)
        self.externaltablepartition = ExternalTablePartitionsProperties(self.today.strftime("%Y"), self.today.strftime("%Y%m"), self.today.strftime("%Y%m%d"))

        # self.year = self.today.strftime("%Y")
        # self.month = self.today.strftime("%Y%m")
        # self.day = self.today.strftime("%Y%m%d")

        # Defined into sub-class
        self.bucket_file_path = "" 

        # Defined into sub-class
        self.bronze_table = ""
        self.bronze_schema = p_bronzeDb_Manager.get_db_username()
        self.bronze_status = None
        
        # Date of last update row
        self.bronze_date_lastupdated_row = None
        
        # Total number of parquet files sent to bucket
        self.total_sent_parquets = 0
        
        # Total size of parquet files
        self.total_sent_parquets_size = 0 
        
        # Total of temporary parquets created when fetching data, before merging for no incremental table
        self.total_temp_parquets = 0
        
        # Total number of rows imported from the source table
        self.total_imported_rows = 0
        
        # Size of importted rows from source
        self.total_imported_rows_size = 0
        self.start = datetime.now()
        self.fetch_start = None
        self.upload_parquets_start = None
        
        # Duration of requests
        self.total_duration = datetime.now() - datetime.now()
        
        # Duration to fetch data and create local parquet files
        self.fetch_duration = datetime.now() - datetime.now()
        
        # Duration to send parquet files to OCI
        self.upload_parquets_duration = datetime.now() - datetime.now()
        
        # If debug = True then we use a dedicated bucket to store parquet files. Bucket name identified into json config
        self.debug = self.bronze_config.isdebugmode()

        # Set bronze bucket settings 
        self.bronze_bucket_proxy = BronzeBucketProxy(self.env,self.bronze_config)
        self.__set_bronze_bucket_proxy__()
         
        # To be set into subclass
        self.source_db = None
        self.source_db_connection = None
        self.source_table_indexes = self.bronze_source_properties.source_table_indexes
        self.bronze_bis_pk = self.bronze_source_properties.bronze_bis_pk

        # For DB source, build select request
        self.where = ''
        self.db_execute_bind = []
        
        if self.bronze_source_properties.table_constraint != None:
            self.where += self.bronze_source_properties.table_constraint
        
        if self.bronze_source_properties.date_criteria != None and self.bronze_source_properties.last_update != None:
            if self.where != "":
                self.where += " AND "
            else:
                self.where += " WHERE "

            self.where += self.bronze_source_properties.date_criteria + " > :1"
            self.db_execute_bind = [self.bronze_source_properties.last_update]

        self.request = ""

        # Set Bronze table settings : table name, bucket path to add parquet files, get index to restart parquet files interation
        self.__set_bronze_table_settings__()
        
        # setup logger to log events errors and COMPLETED into LOG_ table
        self.logger = p_logger
        self.logger.link_to_bronze_source(self)
    

    def set_bronzedb_connection(self, pBronzeDbManager:BronzeDbManager):
        '''Set Bronze DB connection method'''
        self.bronzedb_manager = pBronzeDbManager
        

    def update_total_duration(self):
        '''Update total duration method'''
        self.total_duration = datetime.now() - self.start 


    def get_bronze_config(self):
        '''Get Bronze config method'''
        return self.bronze_config


    def get_bronzedb_manager(self):
        '''Get Bronze DB manager method'''
        return self.bronzedb_manager
    

    def get_post_procedure_parameters(self):
        '''Get post procedure parameters method'''
        if self.bronze_source_properties.bronze_post_proc:
            return (self.bronze_source_properties.bronze_post_proc, self.bronze_source_properties.bronze_post_proc_args)
        else:
            return None
    

    def get_rows_stats(self):
        '''Get rows stats method'''
        return (self.total_imported_rows, self.total_imported_rows_size)


    def get_durations_stats(self):
        '''Get durations stats method'''
        return (self.total_duration,self.fetch_duration, self.upload_parquets_duration)


    def get_parquets_stats(self):
        '''Get parquets stats method'''
        return (self.total_sent_parquets, self.total_sent_parquets_size, self.total_temp_parquets)


    def get_bronze_source_properties(self):
        '''Get Bronze source properties method'''
        return self.bronze_source_properties


    def get_bronze_properties(self):
        '''Get Bronze properties method'''
        return BronzeProperties(self.env, self.bronze_schema, self.bronze_table, self.bronze_bucket_proxy.get_bucket_name(), self.bucket_file_path, self.parquet_file_name_template)


    def get_externaltablepartition_properties(self):
        '''Get external table partition properties method'''
        return self.externaltablepartition
    

    def get_logger(self):
        '''Get logger method'''
        return self.logger


    def get_bronze_status(self):
        '''Get Bronze status method'''
        return self.bronze_status
    

    def set_bronze_status(self, step:str):
        '''Set Bronze status method'''
        self.bronze_status = DICT_STATUS_CODE[step]
        

    def get_bronze_bucket_settings(self):
        '''Get Bronze buckets settings method'''
        return self.bronze_bucket_settings
    

    def get_bucket_parquet_files_sent(self):
        '''Get bucket parquet files sent method'''
        return self.bucket_list_parquet_files_sent
        

    def get_bronze_row_lastupdate_date(self):
        '''Get Bronze row lastupdate date method'''
        return None


    def get_source_table_indexes(self):
        '''Get source table indexes if not already defined'''
        return None
    

    def get_bronze_bis_pk(self):
        '''List columns of primary key based on source table indexes : search for unique index
        unique index name ends with _U{digit} or _PK
        '''
        return None
    

    def isexternalpartionedtable(self):
        '''Is external partioned table'''
        return self.bronze_source_properties.incremental

    def set_parquet_file_list(self, p_parquet_list):
        ''' from list of parquet files, set variable parquet_file_list with format
           format list : [{"file_name":parquet_file_name1,"source_file":parquet_file_name1},{"file_name":parquet_file_name2,"source_file":parquet_file_name2},...]
        '''
        if p_parquet_list:
            self.parquet_file_list = [{"file_name":parquet_file_name,"source_file":parquet_file_name} for parquet_file_name in p_parquet_list]

            ''' Define Bucket file path according last parquet file of list'''
            self.bucket_file_path = self.parquet_file_list[-1].rsplit('/', 1)[0]+'/'

    def set_bronze_date_lastupdate(self,p_bronze_date_lastupdate):
        ''' Set Date_lastupdated '''
        ''' Could be used when cloning bronze table'''
        self.bronze_date_lastupdated_row =p_bronze_date_lastupdate

    def __set_local_workgingdir__(self, path):
        '''Create a temporary directory if it doesn't exist'''
        if not os.path.exists(path):
            os.makedirs(path)

        self.local_workingdir = path
    

    def __update_fetch_row_stats__(self):
        '''Update fetch row stats method'''

        self.total_imported_rows += len(self.df_table_content)
        self.total_imported_rows_size += int(self.df_table_content.memory_usage(deep=True).sum())
        
        if self.fetch_start:
            self.fetch_duration = datetime.now() - self.fetch_start


    def __update_sent_parquets_stats(self):
        '''Update sent parquets stats method'''

        self.total_temp_parquets = len(self.parquet_file_list)
        self.total_sent_parquets = len(self.parquet_file_list_tosend)
        
        for p in self.parquet_file_list_tosend:
            source_file = p["source_file"]
            self.total_sent_parquets_size += os.path.getsize(source_file)
           
            # Deleting temp file
            os.remove(source_file)
       
        if self.upload_parquets_start:
            self.upload_parquets_duration = datetime.now() - self.upload_parquets_start
        else:
            self.upload_parquets_start = datetime.now()

    def __update_copied_parquets_stats(self):
        '''Update copied parquets stats method'''

        self.total_sent_parquets = len(self.parquet_file_list_tosend)

        if self.upload_parquets_start:
            self.upload_parquets_duration = datetime.now() - self.upload_parquets_start
        else:
            self.upload_parquets_start = datetime.now()

    def __add_integration_date_column__(self):
        '''Adding column into every parquet file with System date to log integration datetime'''

        v_integration_time = datetime.now(tz=timezone.utc)
        self.df_table_content["fetch_date"] = v_integration_time
        
        # Adding column FETCH_YEAR, FETCH_MONTH, FETCH_DAY for not external partioned table
        # For this table, the columns are created automatically : columns = subfoler of bucket storage of parquet files
        
        if not self.isexternalpartionedtable():
            v_dict_join = self.get_externaltablepartition_properties()._asdict()
            
            for v_key,v_value in v_dict_join.items():
                self.df_table_content[INVERTED_EXTERNAL_TABLE_PARTITION_SYNONYMS.get(v_key, v_key)] = v_value
       

    def __get_last_parquet_idx_in_bucket__(self):
        '''Check if other parquet files already exist into the same bucket folder
        Get the last id parquet number
        To avoid remplace existing parquet files
        '''

        v_idx = 0
        
        try:
            #bucket = FILESTORAGEFACTORY.create_instance(self.get_bronze_bucket_settings().filestorage_wrapper,**self.get_bronze_bucket_settings()._asdict())
            
            v_bucket = self.bronze_bucket_proxy.get_bucket()
            v_what_to_search = self.get_bronze_properties().bucket_filepath + self.parquet_file_name_template
            v_filter_bucket_func = build_fnmatch_filter('*{}*'.format(v_what_to_search))
            v_list_buckets_files = [obj.name for obj in v_bucket.list_objects(v_filter_bucket_func)]
            
            if v_list_buckets_files:
                v_max_file = max(v_list_buckets_files)
                
                # Eliminate file extension
                v_begin_file_name = v_max_file.split('.')[0]
                
                # span() returns a tuple containing the start-, and end positions of the match.
                
                v_pos = re.search(v_what_to_search, v_begin_file_name).span()[1]
                v_idx = int(v_begin_file_name[v_pos:])
    
        except:
            v_idx = 0

        return v_idx


    def __set_bronze_bucket_proxy__(self):
        '''Define settings for bucket, especially storagename... could depends on subclass'''
        pass
    

    def __set_bronze_table_settings__(self):
        '''Define bronze table name, bucket path to add parquet files, get index to restart parquet files interation'''
        pass
        

    def __clean_temporary_parquet_files__(self):
        '''Clean temporay local parquet files (some could remain from previous failure process'''

        merged_parquet_file_name = self.parquet_file_name_template + PARQUET_FILE_EXTENSION
        merged_parquet_file = os.path.join(self.local_workingdir, merged_parquet_file_name)
        list_files = list_template_files(os.path.splitext(merged_parquet_file)[0])
        
        for f in list_files:
            os.remove(f)


    def __create_parquet_file__(self, verbose=None):
        '''Parquet file si created locally into local_workingdir
        Define the path and filename for the Parquet file, id on PARQUET_IDX_DIGITS digits
        '''
        
        self.parquet_file_id += 1

        parquet_file_name= '{0}{1}{2}'.format(self.parquet_file_name_template, str(self.parquet_file_id).zfill(PARQUET_IDX_DIGITS), PARQUET_FILE_EXTENSION)
        source_file = os.path.join(self.local_workingdir, parquet_file_name)

        try:
            # Create parquet file only if number of dataframe row > 0
            if len(self.df_table_content.index) > 0:
                # Adding date of integration - new column into df
                self.__add_integration_date_column__()
                
                # Write the DataFrame to a Parquet file with the specified compression
                message = "Creating parquet file {0} into local folder {1}".format(parquet_file_name, source_file)
                
                if verbose:
                    verbose.log(datetime.now(tz=timezone.utc),"CREATE_PARQUET", "START", log_message=message)
                
                self.df_table_content.to_parquet(source_file, engine='pyarrow', compression='None')
                
                self.parquet_file_list.append({"file_name": parquet_file_name, "source_file": source_file})
                #print("parquet created")
                
                return True
    
            else:
                message = "No row for {}".format(parquet_file_name)
                
                if verbose:
                    verbose.log(datetime.now(tz=timezone.utc), "CREATE_PARQUET", "START", log_message=message)

                return False
    
        except OSError as err:
            vError = "ERROR Creating parquet file {}".format(parquet_file_name)
            
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_PARQUET", vError,log_message='OS Error : '.format(str(err)))
            self.logger.log(pError=err, pAction=vError)
            
            return False

        except Exception as err:
            vError = "ERROR Creating parquet file {}".format(parquet_file_name)
            
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "CREATE_PARQUET", vError, log_message=str(err))
            self.logger.log(pError=err, pAction=vError)
            
            # Continue can only be used within a loop, so we use pass instead

            return False


    def __custom_select_from_source__(self):
        '''Create select request from source
        If need to encode columns, cast columns on select request
        '''

        if self.source_db:
            full_table_name = self.source_db.get_full_table_name(self.bronze_source_properties.schema, self.bronze_source_properties.table)
            
            if not self.bronze_source_properties.force_encode:
                self.request = "select * from " + full_table_name 
            else:
                if not self.source_db_connection:
                    raise Exception("Error no DB connection")
                (could_custom_select,custom_select_result) = self.source_db.create_select_encode_from_table(full_table_name, self.bronze_source_properties.force_encode)
                
                if could_custom_select:
                    self.request = custom_select_result
                else:
                    raise Exception(custom_select_result)
    
            self.request = self.request + " " + self.where 


    def __sync_bronze_table__(self, p_simulate=False,p_verbose=None):
        '''Sync bronze table method'''

        table = self.bronze_table
        
        try:
            if not self.get_bronzedb_manager().get_db_connection():
                raise Exception("Error no DB connection")
            
            cursor = self.get_bronzedb_manager().get_db_connection().cursor()
            request = 'BEGIN DBMS_CLOUD.SYNC_EXTERNAL_PART_TABLE(table_name =>\'' + table + '\'); END;'

            # Execute a PL/SQL to synchronize Oracle partionned external table
            if p_verbose:
                message = "Synchronizing external partionned table {}".format(table)
                p_verbose.log(datetime.now(tz=timezone.utc), "UPDATE_TABLE", "SYNC", log_message=message)
            
            if not p_simulate:
                cursor.execute(request)
            cursor.close()
            
            return True

        except oracledb.Error as err:
            vError = "ERROR Synchronizing table {}".format(table)
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "SYNC_TABLE", vError, log_message='Oracle DB error :{}'.format(str(err)))
            self.logger.log(pError=err, pAction=vError)
            
            return False

        except Exception as err:
            vError = "ERROR Synchronizing table {}".format(table)
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "SYNC_TABLE", vError, log_message=str(err))
            self.logger.log(pError=err, pAction=vError)
            
            return False


    def __create_bronze_table__(self, p_simulate=False,p_verbose=None):
        '''Create Bronze table method'''
    
        vTable = self.bronze_table
        
        try:
            if not self.get_bronzedb_manager().get_db_connection():
                raise Exception("Error no DB connection")
            
            cursor = self.get_bronzedb_manager().get_db_connection().cursor()
            
            # Dropping table before recreating
            #drop = 'BEGIN EXECUTE IMMEDIATE \'DROP TABLE ' + vTable + '\'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;'
            
            if p_verbose:
                message = "Dropping table {}.{} ".format(self.get_bronzedb_manager().get_db_username(),vTable)
                p_verbose.log(datetime.now(tz=timezone.utc), "DROP_TABLE", "START", log_message=message)

            if not p_simulate:
                ''' Drop existing table by calling Database procedure'''
                vResult_run_proc = self.get_bronzedb_manager().run_proc('LH2_BRONZE_ADMIN_PKG.DROP_TABLE_PROC', *[vTable,self.bronze_config.get_options().bronze_bis_suffixe], p_verbose=p_verbose, p_proc_exe_context=vTable)

                if not vResult_run_proc:
                    raise Exception("ERROR dropping table {}".format(vTable))
            
            # Create external part table parsing parquet files from bucket root (for incremental mode)
            if self.isexternalpartionedtable():
                # Create external part table parsing parquet files from bucket root (for incremental mode)
                root_path = self.get_bronze_properties().bucket_filepath.split("/")[0]+"/"
                create = 'BEGIN DBMS_CLOUD.CREATE_EXTERNAL_PART_TABLE(table_name =>\'' + vTable + '\',credential_name =>\'' + self.bronze_bucket_proxy.get_oci_adw_credential() + '\', file_uri_list =>\'' + self.bronze_bucket_proxy.get_oci_objectstorage_url() + self.get_bronze_properties().bucket + '/o/'+root_path+'*' + self.get_bronze_properties().parquet_template + '*.parquet\', format => \'{"type":"parquet", "schema": "first","partition_columns":[{"name":"fetch_year","type":"varchar2(100)"},{"name":"fetch_month","type":"varchar2(100)"},{"name":"fetch_day","type":"varchar2(100)"}]}\'); END;'
                
                # create += 'EXECUTE IMMEDIATE '+ '\'CREATE INDEX fetch_date ON ' + table + '(fetch_year,fetch_month,fetch_date)\'; END;'
                # Not supported for external table
        
            else:
                # Create external table linked to ONE parquet file (for non incremental mode)
                root_path = self.get_bronze_properties().bucket_filepath
                create = 'BEGIN DBMS_CLOUD.CREATE_EXTERNAL_TABLE(table_name =>\'' + vTable + '\',credential_name =>\'' + self.bronze_bucket_proxy.get_oci_adw_credential() + '\', file_uri_list =>\'' + self.bronze_bucket_proxy.get_oci_objectstorage_url() + self.get_bronze_properties().bucket + '/o/' + root_path + self.get_bronze_properties().parquet_template + '.parquet\', format => \'{"type":"parquet", "schema": "first"}\'); END;'
            
            if p_verbose:
                message = "Creating table {} : {}".format(vTable,create)
                p_verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", "START", log_message=message)
        
            if not p_simulate:
                cursor.execute(create)
           
            '''Alter column type from BINARY_DOUBLE to NUMBER'''
            if p_verbose:
                message = "Altering table columns type {}.{}".format(self.get_bronzedb_manager().get_db_username(), vTable)
                p_verbose.log(datetime.now(tz=timezone.utc), "ALTER_TABLE", "START", log_message=message)
            
            if not p_simulate:
                vResult_run_proc = self.get_bronzedb_manager().run_proc('LH2_BRONZE_ADMIN_PKG.ALTER_TABLE_COLUMN_TYPE_PROC', *[vTable, 'BINARY_DOUBLE', 'NUMBER(38,10)'], p_verbose=p_verbose, p_proc_exe_context=vTable)

                if not vResult_run_proc:
                    raise Exception("ERROR altering table columns type {}.{}".format(self.get_bronzedb_manager().get_db_username(), vTable))
            
            cursor.close()
            
            return True

        except oracledb.Error as err:
            vError= "ERROR Creating table {}".format(vTable)
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", vError, log_message='Oracle DB error : {}'.format(str(err)))
            
            self.logger.log(pError=err, pAction=vError)
            
            return False
    
        except Exception as err:
            vError = "ERROR Creating table {}".format(vTable)
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "CREATE_TABLE", vError, log_message=str(err))
            
            self.logger.log(pError=err, pAction=vError)
            
            return False

    
    def send_parquet_files_to_oci(self, p_simulate=False,p_verbose=None):
        '''Send parquet files to OCI method'''

        self.upload_parquets_start = datetime.now()
        self.__update_sent_parquets_stats()

        if not self.parquet_file_list:
            vError = "WARNING, No parquet files to upload"
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "UPLOAD_PARQUET", vError, log_message="")
           
            self.logger.log(pError=Exception(vError), pAction=vError)
            self.set_bronze_status('parquet_sent')
            
            return True

        self.parquet_file_list_tosend = []
        
        if not self.isexternalpartionedtable():
            # If not incremental mode, merging parquet files into one, before sending
            merged_parquet_file_name = self.parquet_file_name_template+PARQUET_FILE_EXTENSION
            merged_parquet_file = os.path.join(self.local_workingdir, merged_parquet_file_name)
            
            # Use 1st alternative to merge parquet files
            #parquet_files_tomerge = [p["source_file"] for p in self.parquet_file_list]
            #merge_parquet_files(parquet_files_tomerge,merged_parquet_file,removesource=True,self.verbose)
            
            # Use 2nd alternative to merge parquet file based on same root names - based on duckdb
            duckdb_db = DBFACTORY.create_instance(self.bronze_config.get_duckdb_settings().dbwrapper, self.bronze_config.get_configuration_file())
            duckdb_db_connection = duckdb_db.create_db_connection(self.bronze_config.get_duckdb_settings())
            merge_template_parquet_files(os.path.splitext(merged_parquet_file)[0], duckdb_db_connection, True, p_verbose)

            self.parquet_file_list_tosend = [{"source_file":merged_parquet_file,"file_name":merged_parquet_file_name}]
        else:
            self.parquet_file_list_tosend = self.parquet_file_list

        try:
            #bucket = FILESTORAGEFACTORY.create_instance(self.get_bronze_bucket_settings().filestorage_wrapper,**self.get_bronze_bucket_settings()._asdict())
            bucket = self.bronze_bucket_proxy.get_bucket()
        
        except Exception as err:
            self.__update_sent_parquets_stats()
            vError = "ERROR create access to bucket {0}".format(self.bronze_bucket_proxy.get_bucket_name())
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "BUCKET_ACCESS", vError, log_message=str(err))
            
            self.logger.log(pError=err, pAction=vError)
            
            return False
        
        try:    
            for p in self.parquet_file_list_tosend:
                # Sending parquet files
                bucket_file_name = self.get_bronze_properties().bucket_filepath +  p["file_name"]
                source_file = p["source_file"]

                message = "Uploading parquet from {0} into bucket {1}, {2}".format(source_file, self.bronze_bucket_proxy.get_bucket_name(), bucket_file_name)
                
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "UPLOAD_PARQUET", "START", log_message=message)
                
                if not p_simulate:
                    bucket.put_file(bucket_file_name, source_file)
                self.bucket_list_parquet_files_sent.append(bucket_file_name)
    
        except Exception as err:
            self.__update_sent_parquets_stats()
            vError = "ERROR Uplaoding parquet file {0} into bucket {1}, {2}".format(source_file, self.bronze_bucket_proxy.get_bucket_name(), bucket_file_name)
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "UPLOAD_PARQUET", vError, log_message=str(err))
            
            self.logger.log(pError=err, pAction=vError)
            
            return False

        self.__update_sent_parquets_stats()
        self.set_bronze_status('parquet_sent')

        return True

    def copy_parquet_files_to_oci(self, p_bucket_src:str, p_simulate=False,p_verbose=None):
        '''copy parquet files to OCI method'''

        self.upload_parquets_start = datetime.now()
        self.__update_copied_parquets_stats()

        if not self.parquet_file_list:
            vError = "WARNING, No parquet files to copy"

            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "UPLOAD_PARQUET", vError, log_message="")

            self.logger.log(pError=Exception(vError), pAction=vError)
            self.set_bronze_status('parquet_sent')

            return True

        try:
            v_bucket_dest = self.bronze_bucket_proxy.get_bucket()

        except Exception as err:
            self.__update_sent_parquets_stats()
            vError = "ERROR create access to bucket {0}".format(self.bronze_bucket_proxy.get_bucket_name())
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "BUCKET_ACCESS", vError, log_message=str(err))
            self.logger.log(pError=err, pAction=vError)
            return False

        self.parquet_file_list_tosend = self.parquet_file_list

        try:
            for p in self.parquet_file_list_tosend:
                # Copying parquet files
                v_parquet_src = p["file_name"]
                message = "Copying parquet from {0},{1} into bucket {2}".format(v_parquet_src, p_bucket_src.get_filestorage_name(),self.bronze_bucket_proxy.get_bucket_name())
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "COPY_PARQUET", "START", log_message=message)

                if not p_simulate:
                    p_bucket_src.copy_object(v_parquet_src,v_bucket_dest)
                self.bucket_list_parquet_files_sent.append(v_parquet_src)

        except Exception as err:
            self.__update_sent_parquets_stats()
            vError = "ERROR Copying parquet file {0} i{0},{1} into bucket {2}".format(v_parquet_src, p_bucket_src.get_filestorage_name(),
                                                                                    self.bronze_bucket_proxy.get_bucket_name())

            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "COPY_PARQUET", vError, log_message=str(err))

            self.logger.log(pError=err, pAction=vError)

            return False

        self.__update_copied_parquets_stats()
        self.set_bronze_status('parquets_copied')

        return True

    def update_bronze_schema(self, p_simulate=False,p_verbose=None):
        '''Update Bronze schema method'''
    
        try:
            message = "Update Bronze schema {} : External Partioned Table {}".format(self.bronze_table, bool(self.isexternalpartionedtable()))
           
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "UPADATE_BRONZE", "START", log_message=message)
            
            if not self.parquet_file_list:
                message = "No parquet files No update of Bronze needed"
                
                if p_verbose:
                    p_verbose.log(datetime.now(tz=timezone.utc), "UPADATE_BRONZE", "END", log_message=message)
                res = True
    
            elif self.isexternalpartionedtable() and self.get_bronzedb_manager().is_table_exists(self.bronze_table):
                # If incremental mode, if table exists, update (synchronize) external part table
                res = self.__sync_bronze_table__(p_simulate=p_simulate,p_verbose=p_verbose)

            else:
                # If incremental mode,if table not exists,
                # Create external part table linked to "parquet_file_name_template"xxx.parquets files from bucket root
                # If no incremental mode, drop existing table and recreate table linked to uploaded parquet file
                res = self.__create_bronze_table__(p_simulate=p_simulate,p_verbose=p_verbose)
            
            if res:
                self.set_bronze_status('bronze_updated')

            return res

        except Exception as err:
            vError = "ERROR Updating bronze schema, table : {}".format(self.bronze_table)
            
            if p_verbose:
                p_verbose.log(datetime.now(tz=timezone.utc), "UPDATE_BRONZE_SCHEMA", vError, log_message=str(err))
            self.logger.log(pError=err, pAction=vError)
            
            return False


    def pre_fetch_source(self, verbose=None):
        '''TO BE EXECUTED before fetch method'''

        self.set_bronze_status('fetching')
        self.fetch_start = datetime.now()
        
        # Delete old temporary parquet files
        
        self.__clean_temporary_parquet_files__()


    def fetch_source(self, verbose=None):
        '''Request data from source
        and generate local parquet files
        '''
        pass


    def fetch_and_process(self, verbose=None):
        '''Fetch and process method'''
        v_rest_fetch = self.fetch_source(verbose)

        if v_rest_fetch:
            self.set_bronze_status('data_fetched')

        return v_rest_fetch


class BronzeGenerator:
    '''BronzeGenerator class'''

    def __init__(self, pBronzeSourceBuilder:BronzeSourceBuilder, pBronzeExploit:BronzeExploit, pLogger:BronzeLogger=None):
        '''BronzeGenerator constructor'''
        self.v_bronzesourcebuilder = pBronzeSourceBuilder
        self.v_bronzeexploit = pBronzeExploit
        self.v_logger = pLogger


    def generate(self, p_simulate=False,p_verbose=None):
        '''Generate method'''

        while True:
            v_generate_result = False
            v_sourceProperties = self.v_bronzesourcebuilder.get_bronze_source_properties()
            v_dict_update_exploit = dict()
                       
            # 1 Fetch data from source
            self.v_bronzesourcebuilder.pre_fetch_source(p_verbose)
           
            # Update exploit table with status of generator
            v_dict_update_exploit['bronze_status'] = self.v_bronzesourcebuilder.get_bronze_status()
            if not self.v_bronzeexploit.update_exploit(v_dict_update_exploit, p_source=v_sourceProperties,p_simulate=p_simulate):
                break
            
            if not self.v_bronzesourcebuilder.fetch_and_process(p_verbose):
                break
    
            # Update exploit table with status of generator

            v_dict_update_exploit['bronze_status'] = self.v_bronzesourcebuilder.get_bronze_status()
            
            if not self.v_bronzeexploit.update_exploit(v_dict_update_exploit, p_source=v_sourceProperties,p_simulate=p_simulate):
                break
            
            # 2 Upload parquets files to bucket
            if not self.v_bronzesourcebuilder.send_parquet_files_to_oci(p_verbose=p_verbose,p_simulate=p_simulate):
                break
            v_dict_update_exploit['bronze_status'] = self.v_bronzesourcebuilder.get_bronze_status() 
            
            if not self.v_bronzeexploit.update_exploit(v_dict_update_exploit, p_source=v_sourceProperties,p_simulate=p_simulate):
                break
            
            # 3 - Create/Update external table into Autonomous
            if not self.v_bronzesourcebuilder.update_bronze_schema(p_simulate=p_simulate,p_verbose=p_verbose):
                break
            
            v_dict_update_exploit['bronze_status'] = self.v_bronzesourcebuilder.get_bronze_status() 
            
            if not self.v_bronzeexploit.update_exploit(v_dict_update_exploit, p_source=v_sourceProperties,p_simulate=p_simulate):
                break
            
            # 4 Update "last parquet file uploaded", source table index columns, "Last_update" for incremental table integration
            if p_verbose:
                #print(vSourceProperties)
                v_message = "Update Exploit table for {} {} {}".format(v_sourceProperties.name, v_sourceProperties.schema, v_sourceProperties.table)
                p_verbose.log(datetime.now(tz=timezone.utc), "INTEGRATE", "RUNNING", log_message=v_message)
            
            # Get bronze table name to update Exploit loading table
            v_dict_update_exploit['bronze_table_name'] = self.v_bronzesourcebuilder.get_bronze_properties().table
            
            # Get last uploaded parquet file to update Exploit loading table
            v_list = self.v_bronzesourcebuilder.get_bucket_parquet_files_sent()
            v_last_bucket_parquet_file_sent = v_list[-1] if v_list else None
            v_dict_update_exploit['lastuploaded_parquet'] = v_last_bucket_parquet_file_sent
            
            #Get indexes of source table
            v_dict_update_exploit['source_table_indexes'] = self.v_bronzesourcebuilder.get_source_table_indexes()
            
            # If incremental integration, get lastupdate date to update Exploit loading table and build bronze bis merge join            
            if self.v_bronzesourcebuilder.isexternalpartionedtable():
                v_lastupdate_date = self.v_bronzesourcebuilder.get_bronze_row_lastupdate_date()
                v_bronze_bis_pk = self.v_bronzesourcebuilder.get_bronze_bis_pk()
            else:
                v_lastupdate_date = None
                v_bronze_bis_pk = None
            
            if v_lastupdate_date:
                v_dict_update_exploit['last_update'] = v_lastupdate_date
            
            if v_bronze_bis_pk:
                v_dict_update_exploit['bronze_bis_pk'] = v_bronze_bis_pk

            # Set date when bronze table has been updated
            v_dict_update_exploit['bronze_update'] = datetime.now(tz=timezone.utc)
            self.v_bronzesourcebuilder.set_bronze_status('completed')
            v_dict_update_exploit['bronze_status'] = self.v_bronzesourcebuilder.get_bronze_status()
            
            if not self.v_bronzeexploit.update_exploit(v_dict_update_exploit, p_source=v_sourceProperties,p_simulate=p_simulate):
                break
            
            # Run Post integration PLSQL procedure for current source
            v_post_proc_param = self.v_bronzesourcebuilder.get_post_procedure_parameters()
            if v_post_proc_param and not p_simulate:
                v_bronze_table = self.v_bronzesourcebuilder.get_bronze_properties().table
                v_result_post_proc = self.v_bronzesourcebuilder.get_bronzedb_manager().run_proc(v_post_proc_param[0], *v_post_proc_param[1], p_verbose=p_verbose, p_proc_exe_context=v_bronze_table)
                
                if not v_result_post_proc:
                    break
            
            v_generate_result = True

            break

        self.v_bronzesourcebuilder.update_total_duration()

        if v_generate_result:
            if p_verbose:
                #print(vSourceProperties)
                v_message = "Integrating {3} rows from {0} {1} {2} in {4}".format(v_sourceProperties.name, v_sourceProperties.schema,
                                                                         v_sourceProperties.table, self.v_bronzesourcebuilder.get_rows_stats()[0], self.v_bronzesourcebuilder.get_durations_stats()[0])
                p_verbose.log(datetime.now(tz=timezone.utc), "INTEGRATE", "END", log_message=v_message)
            
            if self.v_logger:
                self.v_logger.log()

        return v_generate_result

    def generate_by_cloning_parquets(self, p_bucket_src:AbsBucket,p_parquet_list_src,p_simulate=False,p_verbose=None):
        '''genrate_by_cloning_parquets method'''
        ''' generate external table by cloning parquet files from a bucket'''

        while True:
            v_generate_result = False
            v_sourceProperties = self.v_bronzesourcebuilder.get_bronze_source_properties()
            v_dict_update_exploit = dict()

            # 1 copy parquets to destination bucket
            self.v_bronzesourcebuilder.set_bronze_status('copying_parquets')
            # Update exploit table with status of generator
            v_dict_update_exploit['bronze_status'] = self.v_bronzesourcebuilder.get_bronze_status()
            if not self.v_bronzeexploit.update_exploit(v_dict_update_exploit, p_source=v_sourceProperties,p_simulate=p_simulate):
                break

            self.v_bronzesourcebuilder.set_parquet_file_list(p_parquet_list_src)
            self.v_bronzesourcebuilder.copy_parquet_files_to_oci(p_bucket_src=p_bucket_src,p_simulate=p_simulate,p_verbose=p_verbose)

            # Update exploit table with status of generator
            v_dict_update_exploit['bronze_status'] = self.v_bronzesourcebuilder.get_bronze_status()
            if not self.v_bronzeexploit.update_exploit(v_dict_update_exploit, p_source=v_sourceProperties,p_simulate=p_simulate):
                break

            # 4 - Create/Update external table into Autonomous
            if not self.v_bronzesourcebuilder.update_bronze_schema(p_verbose=p_verbose,p_simulate=p_simulate):
                break

            v_dict_update_exploit['bronze_status'] = self.v_bronzesourcebuilder.get_bronze_status()
            if not self.v_bronzeexploit.update_exploit(v_dict_update_exploit, p_source=v_sourceProperties,p_simulate=p_simulate):
                break

            # 4 Update "last parquet file uploaded", source table index columns, "Last_update" for incremental table integration
            if p_verbose:
                # print(vSourceProperties)
                v_message = "Update Exploit table for {} {} {}".format(v_sourceProperties.name,
                                                                       v_sourceProperties.schema,
                                                                       v_sourceProperties.table)
                p_verbose.log(datetime.now(tz=timezone.utc), "INTEGRATE", "RUNNING", log_message=v_message)

            # Get bronze table name to update Exploit loading table
            v_dict_update_exploit['bronze_table_name'] = self.v_bronzesourcebuilder.get_bronze_properties().table

            # Get last uploaded parquet file to update Exploit loading table
            v_list = self.v_bronzesourcebuilder.get_bucket_parquet_files_sent()
            v_last_bucket_parquet_file_sent = v_list[-1] if v_list else None
            v_dict_update_exploit['lastuploaded_parquet'] = v_last_bucket_parquet_file_sent

            # Get indexes of source table
            v_dict_update_exploit['source_table_indexes'] = self.v_bronzesourcebuilder.get_source_table_indexes()

            # If incremental integration, get lastupdate date to update Exploit loading table and build bronze bis merge join
            if self.v_bronzesourcebuilder.isexternalpartionedtable():
                v_lastupdate_date = self.v_bronzesourcebuilder.get_bronze_row_lastupdate_date()
                v_bronze_bis_pk = self.v_bronzesourcebuilder.get_bronze_bis_pk()
            else:
                v_lastupdate_date = None
                v_bronze_bis_pk = None

            if v_lastupdate_date:
                v_dict_update_exploit['last_update'] = v_lastupdate_date

            if v_bronze_bis_pk:
                v_dict_update_exploit['bronze_bis_pk'] = v_bronze_bis_pk

            # Set date when bronze table has been updated
            v_dict_update_exploit['bronze_update'] = datetime.now(tz=timezone.utc)
            self.v_bronzesourcebuilder.set_bronze_status('completed')
            v_dict_update_exploit['bronze_status'] = self.v_bronzesourcebuilder.get_bronze_status()

            if not self.v_bronzeexploit.update_exploit(v_dict_update_exploit, p_source=v_sourceProperties,p_simulate=p_simulate):
                break

            # Run Post integration PLSQL procedure for current source
            v_post_proc_param = self.v_bronzesourcebuilder.get_post_procedure_parameters()
            if v_post_proc_param and not p_simulate:
                v_bronze_table = self.v_bronzesourcebuilder.get_bronze_properties().table
                v_result_post_proc = self.v_bronzesourcebuilder.get_bronzedb_manager().run_proc(v_post_proc_param[0],
                                                                                                *v_post_proc_param[1],
                                                                                                p_verbose=p_verbose,
                                                                                                p_proc_exe_context=v_bronze_table)

                if not v_result_post_proc:
                    break

            v_generate_result = True

            break

        self.v_bronzesourcebuilder.update_total_duration()

        if v_generate_result:
            if p_verbose:
                # print(vSourceProperties)
                v_message = "Copying {3} parquets from {0} to {1} {2} \n Updating Bronze table {4} {5}  in {6}".format(p_bucket_src.get_filestorage_name(),
                                                                                    self.v_bronzesourcebuilder.get_bronze_properties().bucket,
                                                                                    self.v_bronzesourcebuilder.get_bronze_properties().bucket_filepath,
                                                                                  self.v_bronzesourcebuilder.get_parquets_stats()[0],
                                                                                self.v_bronzesourcebuilder.get_bronze_properties().environment,
                                                                                self.v_bronzesourcebuilder.get_bronze_properties().table,
                                                                                  self.v_bronzesourcebuilder.get_durations_stats()[0])
                p_verbose.log(datetime.now(tz=timezone.utc), "INTEGRATE", "END", log_message=v_message)

            if self.v_logger:
                self.v_logger.log()

        return v_generate_result