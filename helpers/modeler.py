

from pyspark.sql import functions as F

class Modeler():
    def __init__(self,spark):
        self.spark = spark

    @staticmethod
    def dim_demographics(demographics):
        return demographics.select("demo_state_code","demo_state","demo_median_age","demo_male_population","demo_total_population","demo_city"
                                   ,"demo_race","demo_race_count","demo_other_races_count")
