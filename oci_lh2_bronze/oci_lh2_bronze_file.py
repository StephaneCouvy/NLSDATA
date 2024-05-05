import pandas as ps
from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *
import glob


# Define a class BronzeSourceBuilderFile inheriting from BronzeSourceBuilder
class BronzeSourceBuilderFile(BronzeSourceBuilder):
    def __init__(self, pBronze_Config:BronzeConfig, pBronzeDb_Manager:BronzeDbManager,pSrc_name, pSrc_origin_name, pSrc_table_name, pSrc_table_where, pSrc_flag_incr,
                 pSrc_date_where, pSrc_date_lastupdate, pForce_encode, pLogger):
        super().__init__(pBronze_Config, pBronzeDb_Manager, "FILE", pSrc_name, pSrc_origin_name, pSrc_table_name, pSrc_table_where,
                         pSrc_flag_incr, pSrc_date_where, pSrc_date_lastupdate, pForce_encode, pLogger)
        
        # convert src_table_where into dictionary to be used into read file
        if self.bronze_source_properties.table_constraint:
            self.file_read_options = string_to_dictionary(self.bronze_source_properties.table_constraint)
        else:
            self.file_read_options = {}
            
        self.request = "Import files"
        
        # Create connexion to source database
        self.source_filestorage_param = get_parser_config_settings("filestorage")(self.bronze_config.get_configuration_file(),self.bronze_source_properties.name)
        self.source_filestorage = FILESTORAGEFACTORY.create_instance(self.source_filestorage_param.filestorage_wrapper,**self.source_filestorage_param._asdict())

    def __init_bronze_bucket_settings__(self):
        # store all parquet files into same bucket - Not depends on storage name
         # Bucket name ends with '-FILES'
        self.__set_bronze_bucket_settings__('FILES')
        
    def __set_bronze_table_settings__(self):
        #define bronze table name, bucket path to add parquet files, get index to restart parquet files interation
         # Construct the bronze table name based on source name and table
        self.bronze_table = self.bronze_source_properties.type + "_" + self.bronze_source_properties.table.replace(" ", "_")
        # define template name of parquet files
        self.parquet_file_name_template = self.bronze_source_properties.type + "_" + self.bronze_source_properties.table.replace(" ", "_")
        self.parquet_file_id = 0
        # Define the path for storing parquets files in the bucket
        self.bucket_file_path = self.bronze_source_properties.table.replace(" ", "_") + "/" + self.year + "/" + self.month + "/" + self.day + "/"
        # Get the index of the last Parquet file in the bucket
        self.parquet_file_id = self.__get_last_parquet_idx_in_bucket__()
    
    def pre_fetch_source(self,verbose=None):
        super().pre_fetch_source()
        self.source_filestorage_tmp_files = []
        filter_func = build_fnmatch_filter(self.bronze_source_properties.schema)
        source_list_files = [o.name for o in self.source_filestorage.list_objects(filter_func)]
        count_files = len(source_list_files)
        if verbose:
            message = "Getting {0} files from {1} {2}".format(count_files,self.source_filestorage.get_filestorage_name(),self.bronze_source_properties.schema)
            verbose.log(datetime.now(tz=timezone.utc), "PRE_FETCH", "START", log_message=message)
        i=0
        for file in source_list_files:
            i+=1
            if verbose:
                # Log get file
                message = "Getting file {0} to {1}, {2}/{3}".format(file,self.local_workingdir,i,count_files)
                verbose.log(datetime.now(tz=timezone.utc), "PRE_FETCH", "RUN", log_message=message)
            source_tmp_file = self.source_filestorage.get_file(file,self.local_workingdir)
            self.source_filestorage_tmp_files.append(source_tmp_file)

        
    def fetch_source(self, verbose=None):
        """
        This method reads files, transforms them into Parquet files, and updates statistics.
        """
        try:
            if verbose:
                # Log start of fetching
                message = "Extracting data from {0}, {1}, {2}".format(self.bronze_source_properties.name, self.bronze_source_properties.schema, self.bronze_source_properties.table)
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message)
            
            # Iterate through each file in the list of self.source_filestorage_tmp_files
            count_files = len(self.source_filestorage_tmp_files)
            i=0
            for file in self.source_filestorage_tmp_files:
                i+=1
                if verbose:
                    message = "Extracting data from file {0} {1} wih options {2}, {3}/{4}".format(file, self.bronze_source_properties.table,str(self.file_read_options),i,count_files)
                    verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message)
                #check if file exists
                if not os.path.exists(file):
                    raise FileNotFoundError("The path is not valid or the file does not exist")
       
                # Decompress file if it is compressed file (*.gz)
                if os.path.splitext(file)[1] == '.gz':
                    input_file=decompress_gzip_file(file,os.path.dirname(file))
                else:
                    input_file = file
                # Call method to import file
                _df = self.__import_file__(input_file,self.bronze_source_properties.table,**self.file_read_options)

                # Store table content aas force type convertion 
                if self.bronze_source_properties.force_encode:
                    self.df_table_content = _df.astype(self.bronze_source_properties.force_encode)
                else:
                    self.df_table_content = _df
                
                # Create Parquet file
                self.__create_parquet_file__(verbose)

                # Update fetch row statistics
                self.__update_fetch_row_stats__()
                
                #delete temporary source file
                os.remove(input_file)
            return True
        except Exception as err:
            # Log error if fetching fails
            vError = "Extracting error from {0}, file {1}, {2}: {3}".format(self.bronze_source_properties.name, self.bronze_source_properties.schema, self.bronze_source_properties.table, str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err),log_request=self.request)
            self.logger.log(pError=err, pAction=vError)
            self.__update_fetch_row_stats__()
            return False


    # Method to import file (to be implemented)
    def __import_file__(self, *fileargs):
        pass