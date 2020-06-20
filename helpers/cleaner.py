





# Steps :
""":arg
1 - Clean demographics dataset, filling null values withn 0
2 - Clean airports dataset filtering only US airports and discarting anything else that is not an airport. Extract iso regions and cast as float elevation feet.
3 - Clean Immigration dataset by changing labels to comprehensible labels, and recasting data types on columns


"""
from pyspark.sql import functions as F
class Cleaner():
    def __init__(self,spark):
        self.spark = spark

    @staticmethod
    def cleaning_demographics_data(dataFrame):
        # filling null with 0
        dataFrame = dataFrame.na.fill({
            'Median Age':0,'Male Population':0,'Female Population':0,'Total Population':0,'Number of Veterans':0,
            "Foreign-born":0,'Average Household Size':0,'Count':0,
        })
        dataFrame =dataFrame.dropDuplicates()
        return dataFrame

    @staticmethod
    def cleanning_airports_data(dataFrame):
        dataFrame = dataFrame.where((F.col('iso_country') == "US") & (F.col('type').isin('small_airport','medium_airport','large_airport'))).\
        withColumn('iso_region', F.substring(F.col('iso_region'),4,2))

        return dataFrame

    @staticmethod
    def cleanning_immigration_data(dateFrame):
        dataFrame = dateFrame.withColumn("immigration_id", F.col('cicid').cast('integer')).withColumn(
            'immigration_year', F.col('i94yr').cast('integer')). \
            withColumn("immigration_month", F.col("i94mon").cast("integer")).withColumn(
            'immigration_country_cit', F.col('i94cit')).withColumn(
            'immigration_visa_post', F.col('visapost')).withColumn(
            'immigration_expiration_data', F.col('dtaddto')). \
            withColumn("immigration_country_origin", F.col("i94res")).withColumn('immigration_country_port',
                                                                                 F.col("i94port")). \
            withColumn('immigration_country_state', F.col("i94addr")).withColumn('date_base',
                                                                                 F.to_date(F.lit('10/01/1960'),
                                                                                           "MM/dd/yyyy")).withColumn(
            'arrdate_int', F.col('arrdate').cast('int')). \
            withColumn('depdate_int', F.col('depdate').cast('int')).withColumn("immigration_arrival_data", F.expr(
            "date_add(date_base, arrdate_int)")). \
            withColumn("immigration_departure_date", F.expr("date_add(date_base,depdate_int)")) \
            .withColumn("immigration_arrival_mode", F.col("i94mode")).withColumn('age', F.col('i94bir').cast(
            "integer")).withColumn('immigration_visa_code', F.col('i94visa').cast('integer')) \
            .withColumn("immigration_count", F.col('count').cast('int')).withColumn("immigration_admission_number",
                                                                                    F.col('admnum').cast('integer')) \
            .withColumn('immigration_flight_number', F.col('fltno')).withColumn('immigration_visa_type',
                                                                                       F.col('visatype')) \
            .withColumn("bird_year", F.col("biryear").cast("integer")). \
            drop("cicid").drop("i94yr").drop("i94mon").drop("i94cit").drop("i94res").drop("i94port").drop(
            "i94addr").drop("date_base").drop('dtaddto'). \
            drop("arrdate").drop("depdate").drop("i94mode").drop("i94bir").drop("i94visa").drop("fltno").drop(
            "visatype").drop("arrdate_int").drop("depdate_int").drop("admnum").drop("biryear").drop("visapost")
        return dataFrame.select(F.col("immigration_id"), F.col("immigration_country_port"),
                                F.col("immigration_country_state"), F.col("immigration_visa_post"), F.col("matflag"),
                                F.col("immigration_expiration_data")
                                , F.col("gender"), F.col("airline"), F.col("immigration_admission_number"),
                                F.col("immigration_flight_number"), F.col("immigration_visa_type"),
                                F.col("immigration_visa_code"), F.col("immigration_arrival_mode")
                                , F.col("immigration_country_origin"), F.col("immigration_country_cit"),
                                F.col("immigration_year"), F.col("immigration_month"),
                                F.col("bird_year")
                                , F.col("age"), F.col("immigration_count"), F.col("immigration_arrival_data"),
                                F.col("immigration_departure_date"))

    @staticmethod
    def get_countries(countries):
        country = countries \
            .withColumn("code", "immigration_country_code")
        return country

    @staticmethod
    def get_visa(visa):
        visa = visa \
            .withColumn("visa_code", "immigration_visa_code")
        return visa

    @staticmethod
    def get_mode(mode):
        modes = mode \
            .withColumn("immmigration_mode_code", F.col("cod_mode").cast("integer")) \
            .withColumn(" mode_name", "immigration_mode_name")
        return modes

    @staticmethod
    def get_airlines(airlines):
        airlines = airlines \
            .where((F.col("IATA").isNotNull()) & (F.col("Airline_ID") > 1)) \
            .drop("Alias")

        return airlines

