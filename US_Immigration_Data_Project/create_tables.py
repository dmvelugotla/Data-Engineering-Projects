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

import Utilities as util


# Functions to create tables: dimensions and fact

#(1.1): visa_dim
# dimension 
def create_visa_dim(input_data, output_data):
    """ Obtain data pertaining to visa and create a data frame
    Parameters
    ------------ 
        input_data: pyspark dataframe 
        output_data: path where the outputs are stored as parquet files
        
    Return
    -----------
        spark_df: the dataframe built through the function
     
     """
    
    spark_df = input_data.withColumn("visa_category", F.when(F.col('visa_cat_code') == 1, "Business")\
                .when(F.col('visa_cat_code') == 2, "Pleasure")\
                .when(F.col('visa_cat_code') == 3, "Student")\
                .otherwise(None)\
               )\
    .select("visa_cat_code", "visa_category", "visa_type")\
    .dropDuplicates(["visa_cat_code","visa_type"])
    
        
    
#     spark_df = input_data.withColumn("visa_id", F.monotonically_increasing_id())\
#     .withColumn("visa_category", F.when(F.col('visa_cat_code') == 1, "Business")\
#                 .when(F.col('visa_cat_code') == 2, "Pleasure")\
#                 .when(F.col('visa_cat_code') == 3, "Student")\
#                 .otherwise(None)\
#                )\
#     .select("visa_id", "visa_cat_code", "visa_category", "visa_type", "visa_issued_state")\
#     .dropDuplicates(["visa_cat_code","visa_type"," "])

    util.export_files_as_parquet(spark_df, output_data, "visa_dim")


    return spark_df


#(1.2): i94mode_dim
# dimension 
def create_i94mode_dim(input_data, output_data):
    """ Obtain data pertaining to i94mode and create a data frame
    Parameters
    ------------ 
        input_data: pyspark dataframe 
        output_data: path where the outputs are stored as parquet files
        
    Return
    -----------
        spark_df: the dataframe built through the function
     
     """
    
    
    spark_df = input_data.select(F.col("i94_mode")).distinct()
    spark_df = spark_df.withColumn("transport_mode", F.when(F.col('i94_mode') == 1, "Air")\
                .when(F.col('i94_mode') == 2, "Sea")\
                .when(F.col('i94_mode') == 3, "Land")
                .when(F.col('i94_mode') == 9, "Not Reported")
                .otherwise(None)\
               ).dropDuplicates(["i94_mode"])


    util.export_files_as_parquet(spark_df, output_data, "i94_mode_dim")


    return spark_df


#(1.3): status_dim
# dimension
def create_status_dim(input_data, output_data):
    """ Obtain data pertaining to visa and create a data frame
    Parameters
    ------------ 
        input_data: pyspark dataframe 
        output_data: path where the outputs are stored as parquet files
        
    Return
    -----------
        spark_df: the dataframe built through the function
     
     """
    
    spark_df = input_data.select("arrival_flag", "departure_flag", "match_flag").distinct()
    spark_df = spark_df.withColumn("status_flag_id", F.monotonically_increasing_id())\
    .select("status_flag_id", "arrival_flag", "departure_flag", "match_flag")\
    .distinct()
    
    util.export_files_as_parquet(spark_df, output_data, "status_dim")


    return spark_df


#(1.3.a): status_lookup_dim
# dimension
def create_status_lookup_dim(input_data, output_data):
    """ Obtain data pertaining to visa and create a data frame
    Parameters
    ------------ 
        input_data: pyspark dataframe
        output_data: path where the outputs are stored as parquet files
        
    Return
    -----------
        spark_df: the dataframe built through the function
     
     """
                    
    spark_df= input_data.select("status_flag")\
    .withColumn("definition",\
                F.when((F.col("status_flag") == "G"), "Admitted into US")\
                .when((F.col("status_flag") == "O"), "Paroled into US")\
                .when((F.col("status_flag") == "R"), "Departed")\
                .when((F.col("status_flag") == "K"), "Lost I 94 or is deceased")\
                .when((F.col("status_flag") == "N"), "Apprehended")\
                .when((F.col("status_flag") == "T"), "Overstayed")\
                .when((F.col("status_flag") == "Z"), "Adjusted to perm residence")\
                .when(F.col("status_flag").isNull(), None)\
                .otherwise("Other")\
               ).distinct()
               
                          
    util.export_files_as_parquet(spark_df, output_data, "status_lookup_dim")


    return spark_df


#(1.4): date_dim
# dimension
def create_date_dim(input_data, output_data):
    """ Obtain data pertaining to visa and create a data frame
    Parameters
    ------------ 
        input_data: pyspark dataframe 
        output_data: path where the outputs are stored as parquet files
        
    Return
    -----------
        spark_df: the dataframe built through the function
     
     """

    spark_df = input_data.select("dt_arrival",)\
    .withColumn("arr_year", F.year("dt_arrival"))\
    .withColumn("arr_month", F.month("dt_arrival"))\
    .withColumn("arr_weekofyear", F.weekofyear("dt_arrival"))\
    .withColumn("arr_dayofweek", F.dayofweek("dt_arrival"))\
    .withColumn("arr_dayofmonth", F.dayofmonth("dt_arrival"))\
    .dropDuplicates(["dt_arrival"])

    
    util.export_files_as_parquet(spark_df, output_data, "date_dim")


    return spark_df


# (2): demog_bystate_dim
# dimension
def create_demog_dim(input_data, output_data):
    """ Obtain data pertaining to visa and create a data frame
    Parameters
    ------------ 
        input_data: pyspark dataframe 
        output_data: path where the outputs are stored as parquet files
        
    Return
    -----------
        spark_df: the dataframe built through the function
     
     """

    spark_df = input_data.groupBy(F.col("state_code"), F.col("state"))\
    .agg(F.sum("male_pop").alias("male_pop"),\
         F.sum("female_pop").alias("female_pop"),\
         F.sum("total_pop").alias("total_pop"),\
         F.sum("foreign_born").alias("foreign_born"),\
         F.sum("african_american_pop").alias("african_american_pop"),\
         F.sum("asian_pop").alias("asian_pop"),\
         F.sum("hispanic_pop").alias("hispanic_pop"),\
         F.sum("white_pop").alias("white_pop"),\
         F.sum("native_pop").alias("native_pop"), \
         F.round(F.mean("average_household_size"),2).alias("avg_household_size"),\
         F.round(F.mean("median_age"),2).alias("median_age")\
        )
    
    util.export_files_as_parquet(spark_df, output_data, "demog_bystate_dim")


    return spark_df


# #(3): airport_dim
# # dimension
# def create_airport_dim(input_data, output_data):
#     """ Obtain data pertaining to airport and create a data frame
#     Parameters
#     ------------ 
#         input_data: pyspark dataframe 
#         output_data: path for output parquet files
        
#     Return
#     -----------
#         spark_df: output dataframe 
     
#      """

#     spark_df = input_data.select(["ident", "type", "name", "elevation_ft", "continent",\
#                                  "iso_country","iso_region","municipality","gps_code","iata_code",\
#                                  "local_code", "coordinates"]).dropDuplicates(["ident"])


#     util.export_files_as_parquet(spark_df, output_data, "airport_dim")


#     return spark_df



#(3): i94port_dim
# dimension
def create_i94port_dim(input_data, output_data):
    """ Obtain data pertaining to i94port and create a data frame
    Parameters
    ------------ 
        input_data: pyspark dataframe 
        output_data: path where the outputs are stored as parquet files
        
    Return
    -----------
        spark_df: the dataframe built through the function
     
     """

    spark_df = input_data.select(["port_code", "port_city", "port_state"]).dropDuplicates(["port_code"])



    util.export_files_as_parquet(spark_df, output_data, "i94_port_dim")
    
    
    return spark_df


#(4): country_dim
# dimension
def create_country_dim(input_data, output_data):
    """ Obtain data pertaining to i94port and create a data frame
    Parameters
    ------------ 
        input_data: pyspark dataframe 
        output_data: path where the outputs are stored as parquet files
        
    Return
    -----------
        spark_df: the dataframe built through the function
     
     """
    spark_df = input_data.select(F.col("country_code").cast('int').alias("country_code")\
                                   ,"country_name")\
        .dropDuplicates(["country_code"])
        
#     spark_df = spark_df.select(F.col("country_code").cast('int').alias("country_code"))
#     spark_df = input_data.select(["country_code", "country_name"]).dropDuplicates(["country_code"])

    util.export_files_as_parquet(spark_df, output_data, "country_dim")
    
    
    return spark_df



# (5): immigration_fact
# Fact
# def create_immigration_fact(input_data, output_data):
#     """ Obtain data pertaining to i94port and create a data frame
#     Parameters
#     ------------ 
#         input_data: pyspark dataframe 
#         output_data: path where the outputs are stored as parquet files
        
#     Return
#     -----------
#         spark_df: the dataframe built through the function
     
#      """

#     spark_df = input_data.select(["*"]).join(visa_dim, (input_data.i94visa == visa_dim.i94visa) &
#                                              (input_data.visatype == visa_dim.visatype), how = 'full')\
#     .join(i94_mode_dim, (input_data.i94mode == i94_mode_dim.i94_mode), how = 'full')\
#     .join(status_dim, (input_data.entdepa == status_dim.arrival_flag) & \
#           (input_data.entdepd == status_dim.departure_flag) & \
#           (input_data.matflag == status_dim.match_flag), how = 'full')\
#     .join(date_dim, (input_data.dt_arrival == date_dim.dt_arrival), how = 'full')\
#     .join(demog_bystate_dim, (input_data.i94addr == demog_bystate_dim.state_code), how = 'full')\
#     .join(airport_dim, (input_data.i94addr == airport_dim.ident), how = 'full')\
                                 
                                 
#                                  ]).dropDuplicates(["country_code"])

#      print("Building fact table by performing the necessary joins on dim", "\n")
#      immigration_fact = immigration_cleaned\
#      .join(visa_dim,
#            (visa_dim.visa_cat_code == immigration_cleaned.visa_cat_code) &
#            (visa_dim.visa_type == immigration_cleaned.visa_type),
#            how= "left")\
#      .join(i94_mode_dim,
#            (i94_mode_dim.i94_mode == immigration_cleaned.i94_mode),
#            how= "left")\
#      .join(status_dim,
#            (status_dim.arrival_flag == immigration_cleaned.arrival_flag) &
#            (status_dim.departure_flag == immigration_cleaned.departure_flag) &
#            (status_dim.match_flag == immigration_cleaned.match_flag),
#            how= "left")\
#      .join(date_dim, (date_dim.dt_arrival == immigration_cleaned.dt_arrival), how= "left")\
#      .join(demog_bystate_dim, demog_bystate_dim.state_code == immigration_cleaned.arrival_state_code
#            ,how= "left")\
#      .join(i94_port_dim, (i94_port_dim.port_code == immigration_cleaned.i94port_code), how= "left")\
#      .join(country_dim, (country_dim.country_code == immigration_cleaned.country_of_residence),
#            how= "left")\
#      .where(F.col('cicid').isNotNull())\
#      .select("cicid"
#              ,immigration_cleaned["dt_arrival"]
#              ,"entry_year"
#              ,"entry_month"
#              ,"arrival_state_code" 
#              ,immigration_cleaned["visa_type"]
#              ,immigration_cleaned["visa_cat_code"]
#              ,"visa_issued_state"
#              ,immigration_cleaned["i94_mode"]
#              ,immigration_cleaned["i94port_code"]
#              ,"country_of_residence"
#              ,"country_of_birth"
#              ,"age"
#              ,"birth_year"
#              ,"gender"
#              ,"admission_num"
#              ,"occup"
#              ,immigration_cleaned["arrival_flag"]
#              ,immigration_cleaned["departure_flag"]
#              ,immigration_cleaned["match_flag"]
#              ,immigration_cleaned["dt_departure"]
#              ,immigration_cleaned["dt_add_tofile"]
#              ,immigration_cleaned["dt_stay_until"]
#              ,status_dim['status_flag_id'])
    



#     util.export_files_as_parquet(spark_df, output_data, "country_dim")
    
    
#     return spark_df







