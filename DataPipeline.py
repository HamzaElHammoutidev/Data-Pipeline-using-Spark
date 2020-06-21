import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from helpers.gather import Gather
from helpers.cleaner import Cleaner

spark = SparkSession.builder.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").appName("ImmigrationProject").getOrCreate()

# Scope The Project and Gather Data
"""
Since We are processing large files from different soucrs we will use Spark and head() on read 
1  - Check different Files Types from Different Sources : 
    - Csv
    - Dat 
    - Sas [Parquet] 
2 - End Use Casa Purpose : Data warehouse contains Immigrants Cleaned and  Processed Data
3 - Cleaning Data : 
    - We Only need USA Data 
        - Airports 
    - Replace Null with 0 
    - Filter Only Needed columns from Immigration Dataset 
    - Removing Duplicates
    - Rename Immmigration Dataset into Understandable Format 
    - Filter Airlines with only IATA Code 
"""


data_files = {
    "immigration":"data/sas_data",
    "airlines":"data/airlines.dat",
    "airports":"data/airport-codes_csv.csv",
    "cities":"data/cities.csv",
    "countries":"data/countries.csv",
    "mode":"data/mode.csv",
    "demographics":"data/us-cities-demographics.csv",
    "states":"data/us_states.csv",
    "visa":"data/visa.csv"
}
Source = Gather(spark,data_files)

## Data Gathering From Sources Files
airlines = Source.get_airlines_data()
immigration = Source.get_immigration_data()
airports = Source.get_airports_data()
cities = Source.get_cities_data()
countries = Source.get_countries_data()
mode = Source.get_mode_data()
demographics = Source.get_demographics_data()
states = Source.get_states_data()
visa = Source.get_visa_data()


## Data Processing & Cleansing & Modeling
dim_demographics = Cleaner.cleansing_demographics(demographics)
dim_airports = Cleaner.cleansing_airports(airports)
fact_immigration = Cleaner.cleansing_immigration(immigration)
dim_countries = Cleaner.cleansing_countries(countries)
dim_mode = Cleaner.cleansing_mode(mode)
dim_visa = Cleaner.cleansing_visa(visa)
dim_airlines = Cleaner.cleansing_airlines(airlines)

"""
# data after Modeling
print(dim_demographics.head())
print(dim_airports.head())
print(fact_immigration.head())
print(dim_countries.head())
print(dim_mode.head())
print(dim_visa.head())
print(dim_airlines.head())
"""
# loading to Parquet
dim_demographics.write.parquet("data/output/demographics")
dim_airports.write.parquet("data/output/airports")
dim_countries.write.parquet("data/output/countries")
dim_airlines.write.parquet("data/output/airlines")
dim_mode.write.parquet("data/output/mode")
dim_visa.write.parquet("data/output/visa")
fact_immigration = fact_immigration.repartition("immigration_country_state")
fact_immigration.write.partitionBy('immigration_country_state').parquet("data/output/immigration")

