########Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import configparser
import pyspark.sql.functions as F
from pyspark.sql import types as T
from datetime import datetime, timedelta, date
import datetime as dt
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt



# Function to create spark session 
# def create_spark_session():
#     """This function creates a handler for spark session"""
#     spark = SparkSession\
#     .builder\
#     .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
#     .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
#     .enableHiveSupport().getOrCreate() 
    
#     return spark 


# Function to export output files as parquet to the destination path
def export_files_as_parquet(dataframe, output_path, table_name):
    """Function to write output dataframes to parquet
    
    Parameters
    ------------ 
        dataframe - name of the dataframe to be written to the output path
        output_path - name of the output path where the files are to be stored
        table_name - table name to be saved as   
        
    Returns
    ------------ 
    Parquet file
    
    """
    
    print("Exporting {} to {}".format(table_name, output_path + table_name))
    
    dataframe.write.mode("overwrite").parquet(output_path + table_name)
    
    print("Parquet file is now available in: {}".format(output_path + table_name))
    

# Function to clean SAS dates to a standard date format 
def clean_SAS_dates(df):
    """Function to convert SAS dates to a date format  
    
    Initial values: 
    "arrdate" - SAS date (numeric)
    "depdate" - SAS date (numeric)
    
    Converted to:
    "dt_arrival" - yyyy-mm-dd (date)
    "dt_departure" - yyyy-mm-dd (date)
    
    Parameters
    ------------ 
    dataframe 
    
    Return
    ------------
    Updated dataframe with the SAS dates modified to date format 

    """
    
#udf to convert SAS data to string format
    get_date = F.udf(lambda x: (datetime(1960,1,1).date() + timedelta(x)).isoformat() if x else None)
    
    df = df.withColumn("dt_arrival",F.when(F.col("arrdate").isNull(), None)\
                       .when(F.length(F.trim(F.col("arrdate")))<7, None)\
                       .when(F.col("arrdate").isNotNull(), get_date(F.col('arrdate')))\
                       .otherwise(None)\
                      )\
    .withColumn("dt_departure",\
                F.when(F.col("depdate").isNull(), None)\
                .when(F.length(F.trim(F.col("depdate")))<7, None)\
                .when(F.col("depdate").isNotNull(), get_date(F.col("depdate")))\
                .otherwise(None)\
               )
    
    df = df.withColumn("dt_arrival", F.to_date(F.col("dt_arrival")))\
    .withColumn("dt_departure", F.to_date("dt_departure"))
       
    return df


# Function to clean dates from string format to a date
def string_to_date_conv(df):
    """Function to convert dates from string to a date format 
    
    Initial values: 
    "dtadfile" - yyyymmdd (string)
    "dtaddto" - mmddyyyy format + few garbage values (string)
    
    Converted to:
    "dt_add_tofile" - yyyy-mm-dd (date)
    "dt_stay_until" - yyyy-mm-dd (date)
    
    Parameters
    ------------ 
    dataframe 
    
    Return
    ------------
    Updated dataframe with the string dates modified to date format 

    """
    
#     Range of valid dates, months and years
    list_m = ['01', '02', '03','04', '05','06', '07','08', '09', '10', '11', '12']

    list_d = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14'
              ,'15','16', '17', '18', '19', '20', '21', '22','23','24', '25','26', '27'
              ,'28', '29', '30', '31']
    
    list_y = ['19', '20']
    
    
    #udf
    string_to_date = F.udf(lambda x: datetime.strptime(x,"%Y%m%d").strftime('%m-%d-%Y') if x else None)
    
    df = df.withColumn("dt_add_tofile",\
                       F.when(F.col('dtadfile').isNull(), None)\
                       .when((F.length(F.trim(F.col('dtadfile')))<8), None)\
                       .when(F.col('dtadfile').isNotNull(), string_to_date(df.dtadfile))\
                       .otherwise(None)\
                      )\
    .withColumn("dt_stay_until",\
                F.when((F.length(F.trim(F.col('dtaddto')))<8), None)\
                .when(~(F.substring(F.col('dtaddto'),1,2).isin(list_m)) |\
                  ~(F.substring(F.col('dtaddto'),3,2).isin(list_d)) |\
                  ~(F.substring(F.col('dtaddto'),5,2).isin(list_y))\
                      ,None)\
                .when(F.col("dtaddto").isNotNull(), F.to_date('dtaddto','MMddyyyy'))\
                .otherwise(None)\
               )
    
    
    df= df.withColumn("dt_add_tofile",F.to_date(F.col("dt_add_tofile"), "MM-dd-yyyy"))\
    
       
    return df


# Function to remove space between column names and convert to a standard format 
def col_names_conversion(df):
    """Function to remove space/hyphen(-) in column names and replace with an '_' to follow a standard format.
    
    Parameters
    ------------ 
    dataframe 
    
    Return
    ------------
    Updated dataframe with the columns names modified to follow a standard format 

    """
    
    df = df.select([F.trim(F.col(col)).alias(col.replace(' ', '_').replace('-','_').lower()) for col in df.columns])
    
    return df



# Function to remove special characters from columns and split in two
def clean_i94port_names(df):
    """Function to remove ''' at the end of column names and split one column into two  
    
    Parameters
    ------------ 
    dataframe 
    
    Return
    ------------
    Updated dataframe with the columns names modified to follow a standard format 

    """
    
    df = df.select(F.regexp_replace(F.trim(F.col("port_code")),"'", "").alias('port_code'),\
                 F.regexp_replace(F.trim(F.col("port")), "'", "").alias('port'))\
    .withColumn("port_city", F.trim(F.split(F.col('port'), ',').getItem(0)))\
    .withColumn("port_state", F.trim(F.split(F.col('port'), ',').getItem(1)))
   
    return df


# Function to pivot race column values in Demographics data(from rows to columns)
def pivot_demog(df):
    """Function to clean and pivot "race" col values across the dataframe
    
    Parameters
    ------------ 
    dataframe 
    
    Return
    ------------
    Updated dataframe with the race col values names pivoted

    """
    
    df = df.groupBy(F.col("city"), F.col("state"), F.col("median_age"), F.col("male_population"), F.col("female_population"),\
                    F.col("total_population"), F.col("number_of_veterans"), F.col("foreign_born"),\
                    F.col("average_household_size"), F.col("state_code"))\
    .pivot("race")\
    .agg(F.sum("count").cast("integer"))\
    .fillna({"American Indian and Alaska Native" : 0,
             "Asian": 0, 
             "Black or African-American": 0,
             "Hispanic or Latino" : 0,
             "White": 0})    
    
    return df


# Function to change data to appropriate types in Demographics data
def demog_dtypes(df):
    """Function to change data types of cols across the dataframe
    
    Parameters
    ------------ 
    dataframe 
    
    Return
    ------------
    Updated dataframe modified to appropriate data types

    """
    
    df = df.select("city", "state", "state_code","American Indian and Alaska Native",\
                   "Asian","Black or African-American", "Hispanic or Latino", "White",\
                   F.col("median_age").cast("float").alias("median_age"),\
                   F.col("male_population").cast("int").alias("male_pop"),\
                   F.col("female_population").cast("int").alias("female_pop"),\
                   F.col("total_population").cast("int").alias("total_pop"),\
                   F.col("number_of_veterans").cast("int").alias("number_of_veterans"),\
                   F.col("foreign_born").cast("int").alias("foreign_born"),\
                   F.col("average_household_size").cast("float").alias("average_household_size")\
                  )\
    .withColumnRenamed("American Indian and Alaska Native", "native_pop")\
    .withColumnRenamed("Asian", "asian_pop")\
    .withColumnRenamed("Black or African-American", "african_american_pop")\
    .withColumnRenamed("Hispanic or Latino", "hispanic_pop")\
    .withColumnRenamed("White", "white_pop")
    
    return df


# Function to find counts of null values in each column
def null_val_bycol(df):
    """Function to find null values counts in each column in dataframe
    
    Parameters
    ------------ 
    dataframe 
    
    Return
    ------------
    spark dataframe with null value counts by column

    """
    null_counts = df.select(*[(\
                               F.count(F.when(F.isnan(F.col(c)) | F.col(c).isNull() |\
                                              F.col(c).contains('NULL')|(F.col(c) == '')| \
                                              F.col(c).contains('None')\
                                              ,c)\
                                      )\
                               if t not in ("timestamp", "date")\
                               else(F.count(F.when(F.col(c).isNull()\
                                                   , c)\
                                           ))\
                              ).alias(c)\
                              for c, t in df.dtypes if c in df.columns\
                             ])

    
    #     null_counts = df.select([F.count(F.when(F.col(c).contains('None') | \
#                                            F.col(c).contains('NULL') | \
#                                            (F.col(c) == '') |\
#                                            F.col(c).isNull() |\
#                                            F.isnull(F.col(c))|\
#                                            F.isnan(c),c)\
#                                    ).alias(c) \
#                             for c in df.columns])

    return null_counts



# Function to find counts of distinct values in each column
def unique_val_bycol(df):
    """Function to find unique values counts in each column in dataframe
    
    Parameters
    ------------ 
    dataframe 
    
    Return
    ------------
    spark dataframe with unique value counts by column

    """
    
    count_dist = df.agg(*(F.countDistinct(F.col(c)).alias(c) for c in df.columns))
    
    return count_dist



# Function to change data to appropriate types in Demographics data
def immig_dtypes(df):
    """Function to change data types of cols across the dataframe
    
    Parameters
    ------------ 
    dataframe 
    
    Return
    ------------
    Updated dataframe modified to appropriate data types

    """
    
    df = df.select("count", "occup","insnum", "gender", "airline",\
                   "dt_arrival", "dt_departure", "dt_add_tofile", "dt_stay_until",\
                   "i94port", "i94addr", "entdepa", "entdepd", "entdepu",\
                   "matflag", "fltno", "visatype",\
                   F.col("cicid").cast("int").alias("cicid"),\
                   F.col("i94yr").cast("int").alias("entry_year"),\
                   F.col("i94mon").cast("int").alias("entry_month"),\
                   F.col("i94cit").cast("int").alias("country_of_birth"),\
                   F.col("i94res").cast("int").alias("country_of_residence"),\
                   F.col("i94mode").cast("int").alias("i94_mode"),\
                   F.col("i94bir").cast("int").alias("age"),\
                   F.col("i94visa").cast("int").alias("visa_cat_code"),\
                   F.col("visapost").cast("int").alias("visa_issued_state"),\
                   F.col("biryear").cast("int").alias("birth_year"),\
                   F.col("admnum").cast("Decimal(15,0)").alias("admission_num")\
                  )\
    .withColumnRenamed("i94port", "i94port_code")\
    .withColumnRenamed("i94addr", "arrival_state_code")\
    .withColumnRenamed("entdepa", "arrival_flag")\
    .withColumnRenamed("entdepd", "departure_flag")\
    .withColumnRenamed("entdepu", "update_flag")\
    .withColumnRenamed("matflag", "match_flag")\
    .withColumnRenamed("fltno", "flight_no")\
    .withColumnRenamed("visatype", "visa_type")

    return df


# Function to validate row counts in dim and fact tables
def check_row_counts(df, table_name):
    """Function to check row counts in each table in the list
    
    Parameters
    ------------ 
    list 
    
    Return
    ------------
    spark dataframe with unique value counts by column

    """
    
    
    row_count = df.count()
        
    if row_count > 0:
        print(f"Table {table_name} has {row_count} records", "\n")
    else:
        print(f"Table {table_name} has 0 records", "\n")
      
    return row_count


def export_files_to_s3(dataframe, table_name, output_path_s3):
    """Function to write output dataframes to parquet to S3
    
    Parameters
    ------------ 
        dataframe - name of the dataframe to be written to the output path
        output_path - name of the output path where the files are to be stored
        table_name - table name to be saved as   
        
    Returns
    ------------ 
    Parquet file
    
    """
    
    
    print("Exporting {} to S3".format(table_name))
    dataframe.write.mode("overwrite").parquet(output_path_s3 + table_name)
    print("{} parquet files is now available in S3".format(table_name), "\n")

    
    return



