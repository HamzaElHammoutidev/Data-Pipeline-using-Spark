import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from helpers.gather import Gather
from helpers.cleaner import Cleaner
from helpers.validator import Validator
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


# data after Modeling
print(dim_demographics.head())
print(dim_airports.head())
print(fact_immigration.head())
print(dim_countries.head())
print(dim_mode.head())
print(dim_visa.head())
print(dim_airlines.head())

# loading to Parquet
dim_demographics.write.parquet("data/output/demographics")
dim_airports.write.parquet("data/output/airports")
dim_countries.write.parquet("data/output/countries")
dim_airlines.write.parquet("data/output/airlines")
dim_mode.write.parquet("data/output/mode")
dim_visa.write.parquet("data/output/visa")

# Modelizing Fact Table
fact_immigration = fact_immigration \
            .join(dim_demographics, fact_immigration["immigration_country_state"] == dim_demographics["demo_state_code"], "left_semi") \
            .join(dim_airports, fact_immigration["immigration_country_port"] == dim_airports["airport_local_code"], "left_semi") \
            .join(dim_airlines, fact_immigration["immigration_airline"] == dim_airlines["airline_tata"], "left_semi") \
            .join(dim_countries, fact_immigration["immigration_country_origin"] == dim_countries["country_code"], "left_semi") \
            .join(dim_visa, fact_immigration["immigration_visa_code"] == dim_visa["visa_code"], "left_semi") \
            .join(dim_mode, fact_immigration["immigration_arrival_mode"] == dim_mode["mode_code"], "left_semi")

fact_immigration = fact_immigration.repartition("immigration_arrival_date")
fact_immigration.write.partitionBy('immigration_arrival_date').parquet("data/output/immigration")


loading_parquet = {
    "demographics":"data/output/demographics",
    "airports":"data/output/airports",
    "countries":"data/output/countries",
    "airlines":"data/output/airlines",
    "mode":"data/output/mode",
    "visa":"data/output/visa",
    "immigration":"data/output/immigration"
}

# Integrity Validation
validator = Validator(spark,loading_parquet)
fact_immigration = validator.get_facts()
dim_demographics, dim_airports, dim_airlines, dim_countries, dim_visa, dim_mode = validator.get_dimensions()
print(validator.check_integrity(fact_immigration, dim_demographics, dim_airports, dim_airlines, dim_countries, dim_visa, dim_mode)) # True

