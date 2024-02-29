import pandas as pd
from data_ingestion import create_db_engine, query_data, read_from_web_CSV
import logging

class FieldDataProcessor:

    def __init__(self, config_params, logging_level="INFO"):# Make sure to add this line, passing in config_params to the class
        """
    Initialize the DataProcessor object with configuration parameters.

    This method initializes a DataProcessor object with the provided configuration parameters.
    These parameters include the database path, SQL query, column renaming mappings, values renaming mappings,
    and the URL for weather station mapping CSV file.

    Args:
    - config_params (dict): A dictionary containing configuration parameters for the data processing.
        It should include the following keys:
        - 'db_path': Path to the SQLite database.
        - 'sql_query': SQL query to fetch data from the database.
        - 'columns_to_rename': Dictionary mapping original column names to new column names.
        - 'values_to_rename': Dictionary mapping original values to new values for specific columns.
        - 'weather_mapping_csv': URL of the CSV file containing weather station mapping data.
    - logging_level (str, optional): The logging level to use for the logger. Default is "INFO".

    Returns:
    - None
    """
        self.db_path = config_params['db_path']
        self.sql_query = config_params["sql_query"]
        self.columns_to_rename = config_params["columns_to_rename"]
        self.values_to_rename = config_params["values_to_rename"]
        self.weather_map_data = config_params["weather_mapping_csv"]
        
        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None

        # Add the rest of your class code here
     # This method enables logging in the class.
    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        # Use self.logger.info(), self.logger.debug(), etc.


    # let's focus only on this part from now on
    def ingest_sql_data(self):
        """
    Ingest SQL data from the specified database using the provided SQL query.

    This method establishes a connection to the database using the provided database path,
    executes the SQL query to fetch data, and stores the result in a DataFrame attribute.
    It also logs a success message once the data is successfully loaded.

    Returns:
    - pd.DataFrame: DataFrame containing the fetched SQL data.
    """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Sucessfully loaded data.")
        return self.df

    
    def rename_columns(self):
        """
    Rename columns in the DataFrame based on the provided mapping.

    This method renames columns in the DataFrame according to the mapping specified in the `columns_to_rename` attribute.
    It extracts the column names from the configuration and swaps their names. 
    The function logs the swapped columns for reference.

    Returns:
    - None: The function modifies the DataFrame in place.
    """ 
        # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]
        self.logger.info(f"Swapped columns: {column1} with {column2}")       

        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"
        
        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})

    
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
    Apply corrections to specific columns in the DataFrame.

    This method applies corrections to the specified columns in the DataFrame. 
    It first takes the absolute value of the specified column `abs_column`, ensuring positive values.
    Then, it strips whitespace from the specified `column_name` to remove leading and trailing spaces.
    Finally, it applies a mapping of values to rename specified in the `values_to_rename` attribute 
    to the `column_name`, replacing them with the corresponding values from the mapping.

    Args:
    - column_name (str): Name of the column to apply corrections to. Defaults to 'Crop_type'.
    - abs_column (str): Name of the column to take the absolute value of. Defaults to 'Elevation'.

    Returns:
    - None: The function modifies the DataFrame in place.
    """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].str.strip()
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))


    def weather_station_mapping(self):
        """
        Map weather station data to field data.
        This method reads the weather station data from a CSV file located at the URL provided in the
        `weather_map_data` attribute. It then merges this data with the existing DataFrame based on the 
        'Field_ID' column, using a left join. This mapping associates each field with its corresponding 
        weather station, enriching the dataset with additional weather information.
        Returns:
        - None: The function modifies the DataFrame in place, adding the 'Weather_station' column.
        """
        wsd = read_from_web_CSV(self.weather_map_data)
        self.df= self.df.merge(wsd[["Field_ID", "Weather_station" ]], on = "Field_ID", how= 'left')
    

    def process(self):
        """
    Perform data processing steps.

    This method orchestrates the data processing pipeline by sequentially executing the following steps:
    1. Ingest SQL data: Fetches data from an SQL database and loads it into a DataFrame.
    2. Rename columns: Renames specified columns in the DataFrame according to predefined mappings.
    3. Apply corrections: Applies corrections to specific columns in the DataFrame, such as taking the absolute
       value of numerical columns and standardizing categorical values.
    4. Weather station mapping: Associates weather station data with field data by merging the datasets based on
       the 'Field_ID' column.

    After executing these steps, the DataFrame is modified accordingly, incorporating the changes made during 
    data processing.

    Returns:
    - None
    """
        self.ingest_sql_data()
        #Insert your code here
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()