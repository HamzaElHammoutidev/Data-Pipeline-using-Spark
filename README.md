
# Data Pipeline using Spark   
US Immigration data pipeline from different sources using Schema-on-read on Spark for processing large files [/data]:   
   
 - Files Types :   
   - .dat   
   - .csv   
   - .parquet [sas data]  

 - Steps :   
   - Data gathering from different sources 
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
