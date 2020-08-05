# ganit-forex
foreign currency exchange historical data processing in airflow, 
using hive table and hdfs storage and spark for processing. 


airflow folder needs to be treated as airflow home.

This repository has following files/folders.

airflow
	----DAGS
			----ganit_currency_pipeline.py 
				(the dag file placed inside dags folder, which calls 2 scripts from scripts folder when scheduled)
				
			----SCRIPTS
			
					----exchange_rates_ddl.hql
							(the hive ddl file used to create schema of table)
					----forex_processing.py
							(the pyspark code that process the forex data)
			
			----FILES(used for storing intermediate files)
						
					----rates_raw.json
						(file that has raw downloaded data from exchange api)
					
					----rates_filtered.json
						(filtered data after filtering out currencies)