# DynamodbMigrate

The purpose of this migrate directory is the one-time migration of data from AWS dynamoDB database to DataStax Cassandra.  This code relies upon a Travis Crawford github that using AWS SDK to scan the DynamoDB records into a dataframe.  The code in this repository uses this dataframe, modifies the structure, and writes to a Cassandra table.

In order to run this demo, It is assumed that you have the following installed and available on your local system.

  1. Datastax Enterprise 6.7.x
  2. Spark
  3. git
  4. mvn
  5. Travis Crawford repository


# Getting Started with Local DSE/Cassandra


### Starting DSE tarball install on the local OSX or Linux machine (-s starts search, -k starts Spark)

  * `dse cassandra -k` 
  
## Getting and running the demo

  

### Build necessary dependency

  This code relies on a github from Travis Crawford to scan dynamoDB records to a dataframe.  The code 

### This is the Travis Crawford dependency which is forked 

  * Navigate to the directory where you would like to save the code.
  * Execute the following command:
	`https://github.com/jphaugla/spark-dynamodb.git`
  * Build this code
```bash
mvn package
```
  * Install this dependency
```bash
mvn install -Dgpg.skip
```
  * for more information, refer to the readme
https://github.com/jphaugla/spark-dynamodb/blob/master/README.md


### In order to run this demo you will need to download the source from GitHub.

  * Navigate to the migrate directory of the downloaded project
  * Execute the following command:
       `git clone https://github.com/phact/dynamo-cassandra-proxy.git`
  * Build the code 
	`mvn package`
  

### To run 

  * From the migrate directory of the project start the producer app
  
	`./runit.sh`
    
  
####  PROBLEMS with mvn
Needed to clean out jar files on spark and dse dependencies

	rm -rf ~/.ivy2
	rm -rf ~/.m2
## Futures

   * Must pass in the table name as well as the hash_key and sort_key as part of the run.sh script.  Would prefer to only pass the table name and use AWS SDK to get the hash_key and sort_key
   * Run using DSE Analytics.  In this repository and in the spark-dynamodb github, pom.xml files are created to match with spark version in dse analytics 6.7.3.  However, could not get these version to work so abandoned the effort
