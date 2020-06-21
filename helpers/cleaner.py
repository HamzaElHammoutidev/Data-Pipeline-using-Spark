





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
    def cleansing_demographics(dataFrame):
        # filling null with 0
        dataFrame = dataFrame.na.fill({
            'Median Age':0,'Male Population':0,'Female Population':0,'Total Population':0,'Number of Veterans':0,
            "Foreign-born":0,'Average Household Size':0,'Count':0,
        })
        dataFrame =dataFrame.withColumn('demo_state_code',F.col('State Code')).drop('State Code')\
            .withColumn('demo_other_races_count',F.col('Total Population') - F.col('Count'))\
            .withColumn("demo_median_age",F.col("Median Age")).drop("Median Age")\
            .withColumn("demo_male_population",F.col("Male Population")).drop("Male Population")\
            .withColumn("demo_total_population",F.col('Total Population')).drop("Total Population")\
            .withColumn("demo_race_count", F.col('count')).drop("count") \
            .withColumn("demo_city", F.col('city')).drop("city") \
            .withColumn("demo_state", F.col('state')).drop("state") \
            .withColumn("demo_race", F.col('race')).drop("race").\
            select("demo_state_code","demo_state","demo_median_age","demo_male_population","demo_total_population","demo_city"
                                   ,"demo_race","demo_race_count","demo_other_races_count").dropDuplicates()


            #.dropDuplicates()
        return dataFrame

    @staticmethod
    def cleansing_airports(dataFrame):
        dataFrame = dataFrame.where((F.col('iso_country') == "US") & (F.col('type').isin('small_airport','medium_airport','large_airport'))).\
        withColumn('iso_region', F.substring(F.col('iso_region'),4,2)).\
            withColumn("airport_identifiant", F.col("ident")).drop("ident"). \
            withColumn("airport_type", F.col("type")).drop("type"). \
            withColumn("airport_name", F.col("name")).drop("name"). \
            withColumn("airport_elevation_ft", F.col("elevation_ft")).drop("elevation_ft"). \
            withColumn("airport_continent", F.col("continent")).drop("continent"). \
            withColumn("airport_iso_country", F.col("iso_country")).drop("iso_country"). \
            withColumn("airport_region_country", F.col("iso_region")).drop("iso_region"). \
            withColumn("airport_municipality", F.col("municipality")).drop("municipality"). \
            withColumn("airport_gps_code", F.col("gps_code")).drop("gps_code"). \
            withColumn("airport_iata_code", F.col("iata_code")).drop("iata_code"). \
            withColumn("airport_local_code", F.col("local_code")).drop("local_code"). \
            withColumn("airport_coordinates", F.col("coordinates")).drop("coordinates").dropDuplicates()
        return dataFrame

    @staticmethod
    def cleansing_immigration(dateFrame):
        dataFrame = dateFrame.withColumn("immigration_id", F.col('cicid').cast('integer')).\
            withColumn(
            'immigration_year', F.col('i94yr').cast('integer')). \
            withColumn("immigration_month", F.col("i94mon").cast("integer")).\
            withColumn(
            'immigration_country_cit', F.col('i94cit')).\
            withColumn(
            'immigration_visa_post', F.col('visapost')).\
            withColumn(
            'immigration_expiration_data', F.col('dtaddto')). \
            withColumn("immigration_country_origin", F.col("i94res")).withColumn('immigration_country_port',
                                                                                 F.col("i94port")). \
            withColumn('immigration_country_state', F.col("i94addr")).withColumn('date_base',
                                                                                 F.to_date(F.lit('10/01/1960'),
                                                                                           "MM/dd/yyyy")).withColumn(
            'arrdate_int', F.col('arrdate').cast('int')). \
            withColumn('depdate_int', F.col('depdate').cast('int')).\
            withColumn("immigration_arrival_date", F.expr("date_add(date_base, arrdate_int)")). \
            withColumn("immigration_departure_date", F.expr("date_add(date_base,depdate_int)")).\
            withColumn("immigration_arrival_month",F.month(F.col('immigration_arrival_date'))). \
            withColumn("immigration_arrival_year", F.year(F.col('immigration_arrival_date'))). \
            withColumn("immigration_departure_year", F.month(F.col('immigration_departure_date'))). \
            withColumn("immigration_departure_month", F.year(F.col('immigration_departure_date'))). \
            withColumn("immigration_arrival_mode", F.col("i94mode")).\
            withColumn('age', F.col('i94bir').cast("integer")).\
            withColumn('immigration_visa_code', F.col('i94visa').cast('integer')).\
            withColumn("immigration_count", F.col('count').cast('int')).\
            withColumn("immigration_admission_number",F.col('admnum').cast('integer')).\
            withColumn('immigration_flight_number', F.col('fltno')).withColumn('immigration_visa_type',F.col('visatype')) \
            .withColumn("bird_year", F.col("biryear").cast("integer")).\
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
                                , F.col("age"), F.col("immigration_count"), F.col("immigration_arrival_date"),
                                F.col("immigration_departure_date"),F.col("immigration_arrival_month"),
                                F.col("immigration_departure_month"),F.col("immigration_arrival_year"),
                                F.col("immigration_departure_year"))

    @staticmethod
    def cleansing_countries(countries):
        country = countries \
            .withColumn("country_code", F.col('code')).drop("code").\
            withColumn("country_name", F.col('Country_name')).drop("Country_name").dropDuplicates()
        return country

    @staticmethod
    def cleansing_visa(visa):
        visa = visa \
            .withColumn("visa_code", F.col("visa_code")).drop("visa_code").\
            withColumn("visa_name", F.col("visa")).drop("visa").dropDuplicates()
        return visa

    @staticmethod
    def cleansing_mode(mode):
        modes = mode \
            .withColumn("mode_code", F.col("cod_mode").cast("integer")) \
            .withColumn("mode_name", F.col(" mode_name")).drop("cod_mode").drop(" mode_name").dropDuplicates()
        return modes

    @staticmethod
    def cleansing_airlines(airlines):
        airlines = airlines \
            .where((F.col("IATA").isNotNull()) & (F.col("Airline_ID") > 1)) \
            .drop("Alias").\
            withColumn("airline_id",F.col("Airline_ID")). \
            withColumn("airline_name", F.col("Name")).drop("Name"). \
            withColumn("airline_tata", F.col("IATA")).drop("IATA"). \
            withColumn("airline_icao", F.col("ICAO")).drop("ICAO"). \
            withColumn("airline_callsign", F.col("Callsign")).drop("Callsign"). \
            withColumn("airline_country", F.col("Country")).drop("Country"). \
            withColumn("airline_active", F.col("Active")).drop("Active").dropDuplicates()

        return airlines

