# Distributed Data Elaboration

## 🛠 Skills
Hadoop, Spark, Hive, Polybase, SQL Server

## 📝 Requirements
*The project involves the creation of a **Spark job** using the **Scala programming language** for data processing in a distributed **Hadoop** environment. The data to be processed is present in a **Hive** database, in the form of **external tables**, which in turn access the information present within **ORC** files stored on **HDFS**. The data processing logics for each of the tables of interest are reported in an external configuration file. Inside the file there are the destination tables, the respective processing queries that describe the output data extraction logic and other connection parameters for writing job execution information on a monitoring table, in the environment **SQL Server**. Once the data extraction queries have been executed, the results obtained are stored in ORC format on HDFS and, also in this case, it is possible to access the data via external Hive tables, for the respective queries in **HQL**. Furthermore, access and querying of these tables processed in a SQL Server environment is also envisaged. To achieve this, the **Polybase** extension is used which, through the definition of a data source, a file format and the definition of external tables via **DDL**, allows access to the data stored in this specific case on Hadoop.*

## 🔧 Usage
- **Environment configuration**
	1. Make necessary directory in HDFS location:
		```bash
		hdfs dfs -mkdir /<LOCATION>/<DIRECTORY_NAME>`
		```	
	2. Create databases and tables:
		```SQL
		CREATE DATABASE IF NOT EXISTS <DATABASE NAME>;
		DROP TABLE IF EXISTS <DATABASE NAME>.<TABLE NAME>; 
		CREATE EXTERNAL TABLE IF NOT EXISTS <DATABASE NAME>.<TABLE NAME> (...) STORED AS ORC 
		LOCATION '<LOCATION>' TBLPROPERTIES("orc.compress"="COMPRESSION");
		```
- **Execution**
	1. In src/main/external/config.properties file
		1. MANDATORY: set jdbc conntection parameter
		2. OPTIONAL: set other parametries, if necessary
	2. Upload src/main/external/config.properties file to hdfs path $CONFIG_PATH 
	3. Set variables and Spark configuration parameter in file src/main/external/exec_sparkdataelaboration.sh
	4. Exec Spark job with src/main/external/exec_sparkdataelaboration.sh <ARGOMENTO>
- **If necessary, erase project**
	1. Delete database and hive ddl:
		```SQL
		DROP TABLE IF EXISTS <DATABASE NAME>.<TABLE NAME>;
		DROP DATABASE IF EXISTS <DATABASE NAME>
		```
	2. Delete data from HDFS
		```bash
		hadoop fs -rm -r [-skipTrash] /<HDFS PATH>/* 
		```
	3. Re-create database and hive and hive tables with "Environment configuration"