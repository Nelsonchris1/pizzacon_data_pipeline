# pizzacon_data_pipeline
Pizzacon is a New pizza house that delivers to customers only. The IT department is still growing with data analyst. But there were problems that needed solutions to help make better analysis 

### Problem

1. Local DB store will not run query analysis faster with the growing demand for pizza delivered daily
2. Having all data regarding pizzacon in local db store is risky
3. Data analyst would need a faster service to help query data

### Solution
1. Since query time is important, we leverage cloud data warehousing for faster querying
2. For data backup, we create a staging area that stores all daily recorded data
3. Overall create a pipeline that runs daily and moves data from local db to stage area then to cloud data warehouse


## Tools
1. Postgresql
2. Apache Airflow
3. S3
4. Redshift
5. Docker

## Airchitecture
<img src="img/architecture diagram.PNG">


## Result
The image below display several task that ran successfully

# Airflow dag
<img src="img/airflow success.PNG">

# Redshift
<img src="img/sql_task_1.PNG>
