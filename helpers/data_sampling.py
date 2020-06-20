import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").appName("ImmigrationProject").getOrCreate()
sc = SparkContext.getOrCreate()


# Scope The Project and Gather Data
"""
Since We are processing large files from different soucrs we will use Spark and Schema on read 
1  - Check different Files Types from Different Sources : 
    - Csv
    - Dat 
    - Sas 
"""

class DataSampling():
    def __init__(self,spark):
        self.spark = spark

    def parquet_sampling(self,path,sample_number=1000,export_folder="",export_file=""):
        if os.path.isfile(path):
            file = path
            data = spark.read.parquet(file).cache()
            data = data.limit(sample_number)
            data.write.parquet(export_file)
        elif os.path.isdir(path):
            files = path+"/*.parquet"
            files_paths = sc.wholeTextFiles(files).map(lambda x: x[0]).collect()
            data= spark.read.parquet(*files_paths).cache()
            data = data.limit(sample_number)
            #data.write.parquet(export_folder)
            data.write.csv(export_file)