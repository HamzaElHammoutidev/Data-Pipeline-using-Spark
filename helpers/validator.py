from pyspark.sql.functions import *


class Validator:


    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def _get_demographics(self):

        return self.spark.read.parquet(self.paths["demographics"])

    def _get_airports(self):

        return self.spark.read.parquet(self.paths["airports"])

    def _get_airlines(self):

        return self.spark.read.parquet(self.paths["airlines"])

    def _get_countries(self):

        return self.spark.read.parquet(self.paths["countries"])

    def _get_visa(self):

        return self.spark.read.parquet(self.paths["visa"])

    def _get_mode(self):

        return self.spark.read.parquet(self.paths["mode"])

    def get_facts(self):

        return self.spark.read.parquet(self.paths["immigration"])

    def get_dimensions(self):

        return self._get_demographics(), self._get_airports(), self._get_airlines() \
            , self._get_countries(), self._get_visa(), self._get_mode()

    def exists_rows(self, dataframe):

        return dataframe.count() > 0

    def check_integrity(self, fact, dim_demographics, dim_airports, dim_airlines, dim_countries, dim_visa, dim_mode):

        integrity_demo = fact.select(col("immigration_country_state")).distinct() \
                             .join(dim_demographics, fact["immigration_country_state"] == dim_demographics["demo_state_code"], "left_anti") \
                             .count() == 0

        integrity_airports = fact.select(col("immigration_country_port")).distinct() \
                                 .join(dim_airports, fact["immigration_country_port"] == dim_airports["airport_local_code"], "left_anti") \
                                 .count() == 0

        integrity_airlines = fact.select(col("immigration_airline")).distinct() \
                                 .join(dim_airlines, fact["immigration_airline"] == dim_airlines["airline_tata"], "left_anti") \
                                 .count() == 0

        integrity_countries = fact.select(col("immigration_country_origin")).distinct() \
                                  .join(dim_countries, fact["immigration_country_origin"] == dim_countries["country_code"],
                                        "left_anti") \
                                  .count() == 0

        integrity_visa = fact.select(col("immigration_visa_code")).distinct() \
                             .join(dim_visa, fact["immigration_visa_code"] == dim_visa["visa_code"], "left_anti") \
                             .count() == 0

        integrity_mode = fact.select(col("immigration_arrival_mode")).distinct() \
                             .join(dim_mode, fact["immigration_arrival_mode"] == dim_mode["mode_code"], "left_anti") \
                             .count() == 0

        return integrity_demo & integrity_airports & integrity_airlines & integrity_countries\
               & integrity_visa & integrity_mode