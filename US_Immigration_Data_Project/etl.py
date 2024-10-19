import pandas as pd
import numpy as np
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import configparser
import pyspark.sql.functions as F
from pyspark.sql import types as T
from datetime import datetime, timedelta, date
import datetime as dt
import boto3  #to copy files to S3
import os
import logging

# setup logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#importing .py files as modules
import create_tables as create_tables
import Utilities as util

# Read AWS configurations
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.8.5"
# os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
config = configparser.ConfigParser()
config.read('config_file.cfg')
os.environ['AWS_ACCESS_KEY_ID'] =  config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

# Setting up spark session
spark = SparkSession.builder\
        .config("spark.jars.repositories","https://repos.spark-packages.org/")\
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()   


AWS_ACCESS_KEY_ID = config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)


# pd.set_option('display.max_colwidth',-1, 'display.max_rows',200, 'display.max_columns',None)

input_files_S3 = config["S3"]["INPUT_DATA"]
output_files_S3 = config["S3"]["OUTPUT_DATA"]


#####Step 1- Reading input data from the source and data cleaning

## 1.1: Immigration data: 

def process_immig_data(output_path):
    
    """Function to read, process & export 'immigration' SAS data as parquet"""
    
    ###(1)file path to "Immigration" data
    path = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    
    #immigration_spark_df = spark.read.parquet(immig_path)
    print("Reading SAS file into a spark DF")
    immigration_spark_df = spark.read.format('com.github.saurfang.sas.spark').load(path)
   
    
    # limit the number of rows to read for testing purposes
#     n = 50  #******to be removed after******#
#     immigration_spark_df = immigration_spark_df.limit(n) 
    
    print("immigration Schema: ")
    print(immigration_spark_df.printSchema())
    
    print(f'total row counts in immigration: {immigration_spark_df.count()}', "\n")
    
    print("immigration data sample: ","\n")
    immigration_spark_df.show(5)
    
    print("Cleaning dates in SAS format")
    immigration = util.clean_SAS_dates(immigration_spark_df)
    print("Completed transforming dates","\n")
    
    # Convert dates from string format to date (dtadfile & dtaddto)
    print("Convert string to date format","\n")
    immigration = util.string_to_date_conv(immigration)
    print("Completed conversion","\n")
    
    #Handling data types and renaming columns 
    print("Handling data types & changing col names","\n")
    immigration_pre = util.immig_dtypes(immigration)
    immigration_pre.printSchema()
    print("Completed!", "\n")
    
    #Dropping duplicates from 'admission_num'
    print("dropping duplicates from 'admission_num'","\n")
    immigration_cleaned = immigration_pre.dropDuplicates(['admission_num'])
    print("Completed!","\n")
    print("A cleaned version of immigration data is now ready!","\n")
    
    #Write the above dataframe to output(local) as a staging table 
    print("Writing 'immigration_cleaned' as parquet file to the output path")
    immigration_cleaned.write.partitionBy("entry_year","entry_month").mode("overwrite")\
    .parquet(output_path + "staging/" + "immigration_cleaned")
    print("Export complete!","\n")
    
    return immigration_cleaned


## 1.2: Demographics data: 

def process_demog_data(output_path):
    
    """Function to read, process & export 'us-cities-demographics.csv' as parquet"""
    
    print("Reading csv file into a spark DF")
    dm_path = 'us-cities-demographics.csv'
    demographics_spark_df = spark.read.format("csv").option("header", "true")\
    .option("delimiter", ";").load(dm_path)
    
    print("Demographics Schema: ")
    print(demographics_spark_df.printSchema())
    
    print(f'total row counts in demographics: {demographics_spark_df.count()}', "\n")
    
    #Modify column names to follow a standard format 
    print("Converting col names to a standard format")
    demographics = util.col_names_conversion(demographics_spark_df)
    print("Completed!","\n")
    
    print("Demographics data sample: ","\n")
    demographics.show(3)
    
    # Pivot Race from rows to columns across the DF
    print("pivoting data","\n")
    demographics_pivot = util.pivot_demog(demographics)
    print("pivoting complete","\n")
    print("demographics_pivot Schema:", "\n")
    demographics_pivot.printSchema()
    
    # Renaming columns and changing data types in dataframe
    print("Handling data types and renaming columns")
    demographics_cleaned = util.demog_dtypes(demographics_pivot)
    print("Completed!","\n")
        
    #total row count
    print(f'total row counts in demographics: {demographics_cleaned.count()}', "\n")
    print("demographics_cleaned data sample: ", "\n")
    demographics_cleaned.show(5)
    
    print("A cleaned version of demographics data is now ready!", "\n")
    
      #Write the above dataframe to output(local) as a staging table 
    print("Writing 'demographics_cleaned' as parquet file to the output path")
    demographics_cleaned.write.partitionBy("state_code").mode("overwrite")\
    .parquet(output_path + "staging/" + "demographics_cleaned")
    print("Export complete!", "\n")
    
    return demographics_cleaned
 

## 1.3: i94Port data: 
def process_port_data(output_path):  
    
    """Function to read, process & export 'Port_data' as parquet"""
    
    print("Reading csv file into a spark DF")
    port_path = 'Port_data.txt'
    
    #Defining schema
    schema = T.StructType([T.StructField("port_code", T.StringType(), True),\
                       T.StructField("port", T.StringType(), True)])
    i94port_spark_df = spark.read.format('csv').option("delimiter", "=").schema(schema).load(port_path)
   
    print("Port data Schema: ")
    print(i94port_spark_df.printSchema())
    
    print(f'total row counts in port data: {i94port_spark_df.count()}', "\n")
       
    print("Modifying data and column names")
    i94port_cleaned = util.clean_i94port_names(i94port_spark_df)
    i94port_cleaned.columns
    print("Completed!", "\n")
    
    print("i94port data sample: ", "\n")
    i94port_cleaned.show(5, truncate = False)
    print("A cleaned version of 'i94port' data is now ready!", "\n")
    
    #Write the above dataframe to output(local) as a staging table 
    print("Writing 'i94port_cleaned' as parquet file to the output path")
    i94port_cleaned.write.mode("overwrite").parquet(output_path + "staging/" + "i94port_cleaned")
    print("Export complete!", "\n")
    
    return i94port_cleaned
    
    
## 1.4: Country data: 
def process_country_data(output_path):  
    
    """Function to read, process & export 'Country_codes.csv' as parquet"""
    
    print("Reading csv file into a spark DF")
    c_path = 'Country_codes.csv' 
    
    #Defining schema
    schema = T.StructType([T.StructField("country_code", T.StringType(), True),\
                       T.StructField("country_name", T.StringType(), True)])
    
    country_spark_df = spark.read.format('csv').option("delimiter", "=").schema(schema).load(c_path)

    print("Country data Schema: ")
    print(country_spark_df.printSchema())
    
    print(f'total row counts in Country: {country_spark_df.count()}', "\n")

    print("Modifying data and column names")
    country_cleaned = util.col_names_conversion(country_spark_df)
    country_cleaned.columns
    print("Completed!", "\n")
    
    
    print("country_cleaned data sample: ", "\n")
    country_cleaned.show(5, truncate = False)
    print("A cleaned version of 'Country' data is now ready!" ,"\n")
    
    #Write the above dataframe to output (local) as a staging table 
    print("Writing 'country_cleaned' as parquet file to the output path")
    country_cleaned.write.mode("overwrite").parquet(output_path + "staging/" + "country_cleaned")
    print("Export complete!", "\n")
    
    return country_cleaned


#####Step 2- Data modeling - create dim and fact tables:

# Creating dimension tables from staging data in the output folder
def create_dim_tables(output_path):
    
    """Function to extract and build dim tables from staging data"""
    
    print("Reading staging files from output folder", "\n")
    immig_staging = (output_path + "staging/" + "immigration_cleaned")
    immigration_cleaned = spark.read.parquet(immig_staging)

    demog_staging = (output_path + "staging/" + "demographics_cleaned")
    demographics_cleaned = spark.read.parquet(demog_staging)

    port_staging = (output_path + "staging/" + "i94port_cleaned")
    i94port_cleaned = spark.read.parquet(port_staging)

    country_staging = (output_path + "staging/" + "country_cleaned")
    country_cleaned = spark.read.parquet(country_staging)
    
    print("Completed reading staging files ", "\n")

    ## 2.1 visa_dim: 
    # Source: immigration_cleaned
    
    def visa_dim(immigration_cleaned, output_path):
        
        """Function to extract visa_dim and export as parquet:
         Parameters
         ------------ 
         spark dataframe, output path
         """
        print("immigration_cleaned Schema: ")
        print(immigration_cleaned.printSchema())
    
        print("Determining 'visa_dim' table from immigration and exporting it to output path")
        visa_dim = create_tables.create_visa_dim(immigration_cleaned, output_path)
        print("Export complete!", "\n")
    
        print("visa_dim Schema: ")
        print(visa_dim.printSchema())
    
        print("visa_dim data sample: ")
        visa_dim.show(5, truncate = False)
    
        return visa_dim

    ## 2.2 i94_mode_dim: 
    # Source: immigration_cleaned
    def i94_mode_dim(immigration_cleaned, output_path):
        
        """Function to i94_mode_dim visa_dim and export as parquet:
        Parameters
        ------------ 
        spark dataframe ,  output path 
        """
    
        print("Determining 'i94_mode_dim' table from immigration and exporting it to output path")
        i94_mode_dim = create_tables.create_i94mode_dim(immigration_cleaned, output_path)
        print("Export complete!", "\n")
    
        print("i94_mode_dim Schema: ")
        print(i94_mode_dim.printSchema())
    
        print("i94_mode_dim data sample: ")
        i94_mode_dim.show(5, truncate = False)
    
        return i94_mode_dim   
    
    # 2.3 status_dim: 
    # Source: immigration_cleaned
    def status_dim(immigration_cleaned, output_path):
        
        """Function to extract status_dim and export as parquet:
        Parameters
        ------------ 
        spark dataframe ,  output path 
        """

        print("immigration_cleaned Schema:" , "\n")
        print(immigration_cleaned.printSchema())
    
        print("Determining 'status_dim' table from immigration and exporting it to output path")
        status_dim = create_tables.create_status_dim(immigration_cleaned, output_path)
        print("Export complete!", "\n")
    
        print("status_dim Schema: ")
        print(status_dim.printSchema())
    
        print("status_dim data sample: ")
        status_dim.show(5, truncate = False)
    
        return status_dim


    ## 2.3.(a) status_lookup_dim: 
    def status_lookup_dim(output_path):
        
        """Function to extract status_lookup_dim and export as parquet"""
    
        schema = T.StructType([T.StructField("status_flag", T.StringType(), True)])
        data = [("G",), ("O",), ("R",), ("K",), ("N",), ("T",), ("Z",), (None,)]
                         
        status_lookup = spark.createDataFrame(data = data, schema = schema)
    
        print("Creating status_lookup_dim and exporting it the output path", "\n")
        status_lookup_dim = create_tables.create_status_lookup_dim(status_lookup,output_path)
        print("Export complete!", "\n")
    
        print("status_lookup_dim Schema: ")
        print(status_lookup_dim.printSchema())
    
        print("status_lookup_dim data sample: ")
        status_lookup_dim.show(5, truncate = False)
    
        return status_lookup_dim

    
    ## 2.4 date_dim: 
    # Source: immigration_cleaned
    def date_dim(immigration_cleaned, output_path):
        
        """Function to extract date_dim and export as parquet:
        Parameters
        ------------ 
        spark dataframe ,  output path 
        """

        print("Determining 'date_dim' table from immigration and exporting it to output path", "\n")
        date_dim = create_tables.create_date_dim(immigration_cleaned, output_path)
        print("Export complete!", "\n")
    
        print("date_dim Schema: " )
        print(date_dim.printSchema())
    
        print("date_dim data sample: ")
        date_dim.show(5, truncate = False)
    
        return date_dim

    
    ## 2.5 demog_bystate_dim: 
    # Source: demographics_cleaned
    def demog_bystate_dim(demographics_cleaned, output_path):
        
        """Function to extract demog_bystate_dim and export as parquet:
        Parameters
        ------------ 
        spark dataframe ,  output path 
        """
    
        print("demographics_cleaned Schema: ")
        print(demographics_cleaned.printSchema())
    
        print("Determining 'demog_bystate_dim' table from demographics_cleaned and exporting it to output path", "\n")
        demog_bystate_dim = create_tables.create_demog_dim(demographics_cleaned, output_path)
        print("Export complete!", "\n")
    
        print("date_dim Schema: ")
        print(demog_bystate_dim.printSchema())
    
        print("demog_bystate_dim data sample: ")
        demog_bystate_dim.show(5, truncate = False)
    
        return demog_bystate_dim
    

    ## 2.6 i94_port_dim: 
    # Source: i94port_cleaned
    def i94_port_dim(i94port_cleaned, output_path):
        
        """Function to extract i94_port_dim and export as parquet:
        Parameters
        ------------ 
        spark dataframe ,  output path 
        """
    
        print("i94port_cleaned Schema:" , "\n")
        print(i94port_cleaned.printSchema())
    
        print("Determining 'i94_port_dim' table from i94port_cleaned and exporting it to output path", "\n")
        i94_port_dim = create_tables.create_i94port_dim(i94port_cleaned,output_path)
        print("Export complete!", "\n")
    
        print("i94_port_dim Schema: ")
        print(i94_port_dim.printSchema())
    
        print("i94_port_dim data sample: ")
        i94_port_dim.show(5, truncate = False)
    
        return i94_port_dim


    ## 2.7 country_dim: 
    # Source: country_cleaned
    def country_dim(country_cleaned, output_path):
        
        """Function to extract country_dim and export as parquet:
        Parameters
        ------------ 
        spark dataframe ,  output path 
        """
    
        print("country_cleaned Schema: ")
        print(country_cleaned.printSchema())
    
        print("Determining 'country_dim' table from country_cleaned and exporting it to output path", "\n")
        country_dim = create_tables.create_country_dim(country_cleaned, output_path)
        print("Export complete!", "\n")
    
        print("country_dim Schema: ")
        print(country_dim.printSchema())
    
        print("country_dim data sample: ")
        country_dim.show(5, truncate = False)
    
        return country_dim
    
    #Function calls
    visa_dim(immigration_cleaned, output_path)
    i94_mode_dim(immigration_cleaned, output_path)
    status_dim(immigration_cleaned, output_path)
    status_lookup_dim(output_path)
    date_dim(immigration_cleaned, output_path)
    demog_bystate_dim(demographics_cleaned, output_path)
    i94_port_dim(i94port_cleaned, output_path)
    country_dim(country_cleaned, output_path)
    
    return 
    
##### Step3: Creating fact table from dimension and staging data

def create_immigration_fact(output_path,output_path_s3):
    
    """Function to create immigration_fact and export as parquet"""
    
    print("Reading immigration file from output", "\n")
   
    immig_staging = (output_path + "staging/" + "immigration_cleaned")
    immigration_cleaned = spark.read.parquet(immig_staging)
    
    demog_staging =  (output_path + "staging/" + "demographics_cleaned")
    demographics_cleaned = spark.read.parquet(demog_staging)
    
    port_staging = (output_path + "staging/" + "i94port_cleaned") 
    i94port_cleaned = spark.read.parquet(port_staging)
    
    country_staging = (output_path + "staging/" + "country_cleaned") 
    country_cleaned = spark.read.parquet(country_staging)
    
    print("Reading dim tables from output", "\n")
    visa_dim = spark.read.parquet(output_path + 'visa_dim')
    i94_mode_dim = spark.read.parquet(output_path + 'i94_mode_dim')
    status_dim = spark.read.parquet(output_path + 'status_dim')
    status_lookup_dim = spark.read.parquet(output_path + 'status_lookup_dim')
    date_dim = spark.read.parquet(output_path + 'date_dim')
    demog_bystate_dim = spark.read.parquet(output_path + 'demog_bystate_dim')
    i94_port_dim = spark.read.parquet(output_path + 'i94_port_dim')
    country_dim = spark.read.parquet(output_path + 'country_dim')
     
    print("Building fact table by performing the necessary join on dim tables", "\n")
    immigration_fact = immigration_cleaned\
    .join(visa_dim,
          (visa_dim.visa_cat_code == immigration_cleaned.visa_cat_code) &
          (visa_dim.visa_type == immigration_cleaned.visa_type),
          how= "left")\
    .join(i94_mode_dim,
          (i94_mode_dim.i94_mode == immigration_cleaned.i94_mode),
          how= "left")\
    .join(status_dim,
          (status_dim.arrival_flag == immigration_cleaned.arrival_flag) &
          (status_dim.departure_flag == immigration_cleaned.departure_flag) &
          (status_dim.match_flag == immigration_cleaned.match_flag),
          how= "left")\
    .join(date_dim, (date_dim.dt_arrival == immigration_cleaned.dt_arrival), how= "left")\
    .join(demog_bystate_dim, demog_bystate_dim.state_code == immigration_cleaned.arrival_state_code
          ,how= "left")\
    .join(i94_port_dim, (i94_port_dim.port_code == immigration_cleaned.i94port_code), how= "left")\
    .join(country_dim, (country_dim.country_code == immigration_cleaned.country_of_residence),
          how= "left")\
    .where(F.col('cicid').isNotNull())\
    .select("cicid"
            ,immigration_cleaned["dt_arrival"]
            ,"entry_year"
            ,"entry_month"
            ,"arrival_state_code" 
            ,immigration_cleaned["visa_type"]
            ,immigration_cleaned["visa_cat_code"]
            ,"visa_issued_state"
            ,immigration_cleaned["i94_mode"]
            ,immigration_cleaned["i94port_code"]
            ,"country_of_residence"
            ,"country_of_birth"
            ,"age"
            ,"birth_year"
            ,"gender"
            ,"admission_num"
            ,"occup"
            ,immigration_cleaned["arrival_flag"]
            ,immigration_cleaned["departure_flag"]
            ,immigration_cleaned["match_flag"]
            ,immigration_cleaned["dt_departure"]
            ,immigration_cleaned["dt_add_tofile"]
            ,immigration_cleaned["dt_stay_until"]
            ,status_dim['status_flag_id'])
    
    print(f'total row counts in immigration_fact: {immigration_fact.count()}', "\n")
        
    print("immigration_fact Schema:" , "\n")
    print(immigration_fact.printSchema())
    
    print("immigration_fact data sample: " "\n")
    immigration_fact.show(5, truncate = False)
    
# ##     print("Writing 'immigration_fact' as parquet file to the output path")
# ##     immigration_fact.write.mode("overwrite").parquet(output_path)
# ##    print("'immigration_fact' is now available in the destination folder")

    ###Function to export files to S3
    tables = {"immigration_cleaned": immigration_cleaned,
              "demographics_cleaned": demographics_cleaned,
              "i94port_cleaned": i94port_cleaned,
              "country_cleaned": country_cleaned,
              "immigration_fact": immigration_fact,
              "visa_dim" : visa_dim,
              "i94_mode_dim": i94_mode_dim, 
              "status_dim": status_dim,
              "status_lookup_dim": status_lookup_dim,
              "date_dim": date_dim,
              "demog_bystate_dim": demog_bystate_dim,
              "i94_port_dim": i94_port_dim,
              "country_dim": country_dim }
    
    for tab_name, df in tables.items():
        util.export_files_to_s3(df, tab_name, output_path_s3)
    
    
    ### Data quality checks on dimension and fact tables
    tab_dict = {"immigration_fact": immigration_fact,
              "visa_dim" : visa_dim,
              "i94_mode_dim": i94_mode_dim, 
              "status_dim": status_dim,
              "status_lookup_dim": status_lookup_dim,
              "date_dim": date_dim,
              "demog_bystate_dim": demog_bystate_dim,
              "i94_port_dim": i94_port_dim,
              "country_dim": country_dim }

    for tab_name, df in tab_dict.items():
        util.check_row_counts(df,tab_name)
        
    
    print("All exports complete, data is now available in S3!")
    
    
    return
   

    
def main():
    
    """Load PARQUET files for immigration, demographics, port and country codes from input,
       process and extract it to: visa_dim, i94_mode_dim, status_dim, status_lookup_dim, date_dim
       ,demog_bystate_dim, i94_port_dim, country_dim & immigration_fact tables       
       and store the derived tables to the output paths as parquets"""
    
    """Assigning values to the variables in function calls"""
    input_path = input_files_S3
    output_path_s3 = output_files_S3
    output_path = 'output-data/'   #local 

    
    """Function calls to process input data: passing inputs to functions"""
    process_immig_data(output_path)
    process_demog_data(output_path)
    process_port_data(output_path)
    process_country_data(output_path)
    create_dim_tables(output_path)
    create_immigration_fact(output_path, output_path_s3)

    
if __name__ == "__main__":
    main()












