import pandas as ps
from nlsdata.oci_lh2_bronze.oci_lh2_bronze import *
import glob


# Define a class BronzeSourceBuilderFile inheriting from BronzeSourceBuilder
class BronzeSourceBuilderFile(BronzeSourceBuilder):
    def __init__(self, br_config, src_name, src_origin_name, src_table_name, src_table_where, src_flag_incr,
                 src_date_where, src_date_lastupdate, force_encode, logger):
        super().__init__(br_config, "FILE", src_name, src_origin_name, src_table_name, src_table_where,
                         src_flag_incr, src_date_where, src_date_lastupdate, force_encode, logger)
        # convert src_table_where into dictionary to be used into pandas read file
        if self.src_object_constraint:
            self.file_read_options = string_to_dictionary(self.src_object_constraint)
        else:
            self.file_read_options = {}
            
        # Construct the bronze table name based on source name and table
        self.bronze_table = self.src_name + "_" + self.src_table.replace(" ", "_")
        # Define the path for storing files in the bucket
        self.bucket_file_path = self.src_table.replace(" ", "_") + "/" + self.year + "/" + self.month + "/" + self.day + "/"
        # Get the index of the last Parquet file in the bucket
        self.parquet_file_id = self.__get_last_parquet_idx_in_bucket__()
        self.request = "Import files"

    # Method to fetch data from the source
    def fetch_source(self, verbose=None):
        """
        This method reads files, transforms them into Parquet files, and updates statistics.
        """
        try:
            if verbose:
                # Log start of fetching
                message = "Extracting data from file {0}, {1}, {2}".format(self.src_name, self.src_schema, self.src_table)
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", "START", log_message=message)
            # Use glob.glob() to retrieve all files in the directory
            all_files = glob.glob(self.src_schema)
            # Iterate through each file in the list of all_files
            for file in all_files:
                #check if file exists
                if not os.path.exists(file):
                    raise FileNotFoundError("The path is not valid or the file does not exist")
       
                # Call method to import file
                _df = self.__import_file__(file,self.src_table,**self.file_read_options)

                # Store table content as string types
                self.df_table_content = _df.astype('string')

                # Create Parquet file
                self.__create_parquet_file__()

                # Update fetch row statistics
                self.__update_fetch_row_stats__()
            return True
        except Exception as err:
            # Log error if fetching fails
            vError = "Extracting error from {0}, file {1}, {2}: {3}".format(self.src_name, self.src_schema, self.src_table, str(err))
            if verbose:
                verbose.log(datetime.now(tz=timezone.utc), "FETCH", vError, log_message=str(err),log_request=self.request)
            self.logger.log(error=err, action=vError)
            self.__update_fetch_row_stats__()
            return False


    # Method to import file (to be implemented)
    def __import_file__(self, *fileargs):
        pass