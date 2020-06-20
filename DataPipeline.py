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
Since We are processing large files from different soucrs we will use Spark and Schema on read 
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

#airlines = Source.get_airlines_data()
immigration = Source.get_immigration_data()
#airports = Source.get_airports_data()
#cities = Source.get_cities_data()
#countries = Source.get_countries_data()
#mode = Source.get_mode_data()cleanning_immigration_data
#demographics = Source.get_demographics_data()
#states = Source.get_states_data()
#visa = Source.get_visa_data()

#demographics = Cleaner.cleaning_demographics_data(demographics)
#airports = Cleaner.cleanning_airports_data(airports)
#print(immigration.describe())
immigration = Cleaner.cleanning_immigration_data(immigration)
print(immigration.head())
#print(spark.read.csv("").select(F.col('type')))




