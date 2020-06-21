
# Data Pipeline using Spark   

Bulding US Immigrtion data pipleline that pulls data from differents sources and create star schema ETL. Spark used for processing large files follwoing Schema-on-read principle. Data can be found at [/data]
 
 - Files Types :   
   - .dat   
   - .csv   
   - .parquet [sas data]  

 - Steps :   
   - Data gathering from all sources 
   - Data exploration 
   - End use-case definition
   - Data processing and cleansing
	   - Checking missing values and duplicates
   - Conceptual data modeling 
	   - Data denormalization : multiple joins should be avoided when we deal with large datasets - Star Schema
   - Creating data model and running ETL 
   - Data quality checks
   
# Sources Datasets : 
![Data Lineage](https://github.com/HamzaElHammoutidev/Data-Pipeline-using-Spark/blob/master/SourcesImage.png)
#	Linking Datasets : 
![Data Lineage](https://github.com/HamzaElHammoutidev/Data-Pipeline-using-Spark/blob/master/SnowflakeSchema.png)
# Conceptual Data Modeling : 
![Data Lineage](https://github.com/HamzaElHammoutidev/Data-Pipeline-using-Spark/blob/master/StarSchema.png)
 - 
 - Notes : 
	 - Fact Table [Immigration] is partitioned by [Arrival Date], if we want to update our dataset daily using Airflow, data can be easily ingested
