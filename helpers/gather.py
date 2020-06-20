import findspark
findspark.init()


from pyspark.sql.types import StructField,StructType, IntegerType,StringType
from pyspark import SparkContext
import os
class Gather():

    sc = SparkContext.getOrCreate()
    def __init__(self,spark,paths):
        self.spark = spark
        self.paths = paths

    # Reading Files Types Helpers
    def read_csv_data(self,file_path,delimiter=","):
        return self.spark.read.option("delimiter",delimiter).option('header','true').csv(file_path)



    def get_airlines_data(self):
        schema = StructType([
            StructField("Airline_ID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Alias", StringType(), True),
            StructField("IATA", StringType(), True),
            StructField("ICAO", StringType(), True),
            StructField("Callsign", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Active", StringType(), True)])

        return self.spark.read.csv(self.paths["airlines"],header=False,schema=schema)

    def get_airports_data(self):
        return self.read_csv_data(self.paths["airports"])
    
    def get_cities_data(self):
        return self.read_csv_data(self.paths["cities"])

    def get_countries_data(self):
        return self.read_csv_data(self.paths["countries"])

    #def get_immigration_data(self):
    #    return self.read_csv_data(self.paths["immigration"])
    def get_immigration_data(self):
        if os.path.isfile(self.paths["immigration"]):
            file = (self.paths["immigration"])
            data = self.spark.read.parquet(file).cache()
            data = data.limit(1000)
            return data
        elif os.path.isdir(self.paths["immigration"]):
            files = (self.paths["immigration"])+"/*.parquet"
            files_paths = Gather.sc.wholeTextFiles(files).map(lambda x: x[0]).collect()
            data= self.spark.read.parquet(*files_paths).cache()
            data = data.limit(1000)
            return data

    
    def get_mode_data(self):
        return self.read_csv_data(self.paths["mode"])

    def get_demographics_data(self):
        return self.read_csv_data(self.paths["demographics"],delimiter=";")

    def get_states_data(self):
        return self.read_csv_data(self.paths["states"])

    def get_visa_data(self):
        return self.read_csv_data(self.paths["visa"])


