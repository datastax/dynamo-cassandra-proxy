package com.datastax.powertools.migrate
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.datastax.driver.core.exceptions.AlreadyExistsException
import com.github.traviscrawford.spark.dynamodb._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal


object dynamoDB {

  private val log = LoggerFactory.getLogger(this.getClass)
  def getKeys(table_name: String): List[String] = {
    log.info("Getting description for %s\n\n", table_name)
    val ddb = AmazonDynamoDBClientBuilder.defaultClient
    var keyList = new ListBuffer[String]()
    try {
      val table_info = ddb.describeTable(table_name).getTable
      val keyschema = table_info.getKeySchema()
      log.info("Keys")
      import scala.collection.JavaConversions._
      for (k <- keyschema) {
        log.info(k.getAttributeName + "(" + k.getKeyType + ")\n")
        keyList += k.getAttributeName
      }
    } catch {
      case NonFatal(err) =>
        log.error(s"Failed getting table information for: ${table_name}", err)
    }
    log.info("\nDone!")
    (keyList.toList)
  }

    def main(args: Array[String]) {

    log.info("entered main")
    var table_name = "na"

    if (args.length > 0) {
      table_name = args(0)
    } else {
      log.info("not enough parameters for job $args.length")
    }
    log.info("entered main " + table_name)

    val sparkJob = new SparkJob()
    try {
      sparkJob.runJob(table_name)
    } catch {
      case ex: Exception =>
        log.info("error in main running spark job")
    }
  }


  class SparkJob extends Serializable {

    log.info("before build spark session")

    def runJob(table_name: String) = {
      val appName = "DynamoReader"
      val sparkSession =
        SparkSession.builder
          .appName(appName)
          .config("spark.cassandra.connection.host", "node0")
          .getOrCreate()

      log.info(s"before read dynamodb " + table_name)
      var dynamoDF  = sparkSession.emptyDataFrame
      try {
        dynamoDF = sparkSession.read.dynamodb(table_name)
      } catch {
        case ex: Exception =>
          log.error("Did not find " + table_name + " in DynamoDB")
          System.exit (2)
      }
      dynamoDF.printSchema()
//	 this caused breakage due to type conversions
//      dynamoDF.show(5)
      val keycols = getKeys(table_name)
      println(s"print key columns")
      keycols.foreach {println}
      //  hash_key is always first
      // initialize sort key as it may be null
      var sort_key = "na"
      if (keycols.length > 1) sort_key = keycols(1).toLowerCase
      val hash_key = keycols(0).toLowerCase()
      //  gets all columns labels into a list, this will be used for list of json columns
      val cols = dynamoDF.columns.toSeq
     // remove the hash_key and the sort_key as they should not be in json string
      val othercols = cols.filterNot(keycols.toSet)
      // val othercols = cols.filterNot(x => x == hash_key).filterNot(x => x == sort_key)
      println(s"print columns for cols")
      cols.foreach {println}
      println(s"print columns for othercols")
      othercols.foreach {println}
      //  create string to be used within the expression to add the structype column
      val expressString = "(" + othercols.mkString(",") + ")"
      //  add the structure column and the json_blob column
      val newDF = dynamoDF.withColumn("structure",expr(expressString))
        .withColumn("json_blob", expr("to_json(structure)"))
      //  this show causes breakage on long to string conversion
      // newDF.show(2)
      newDF.printSchema()
      //  Only need to write out the three columns
      var writeDF  = sparkSession.emptyDataFrame
      if (keycols.length > 1) {
        writeDF = newDF.select(col(hash_key), col(sort_key), col("json_blob"))
      } else {
        writeDF = newDF.select(col(hash_key),  col("json_blob"))
      }
      writeDF.printSchema()
      println(s"before create cassandra table, $table_name, $hash_key, $sort_key")
      try {
        if (keycols.length > 1) {
            writeDF.createCassandraTable("testks",table_name.toLowerCase(),partitionKeyColumns = Some(Seq(hash_key))
                ,clusteringKeyColumns = Some(Seq(sort_key)))
        } else {
          writeDF.createCassandraTable("testks",table_name.toLowerCase(),partitionKeyColumns = Some(Seq(hash_key))
            )
        }
      } catch {
        case ex: AlreadyExistsException => log.info(table_name + " already existed so did not recreate");
        case ex: Exception => ex.printStackTrace();
      }
      log.info(s"before write cassandra, $table_name, $hash_key, $sort_key")
      try {
      writeDF.write.cassandraFormat(table_name.toLowerCase, "testks").mode(SaveMode.Append).save()
      } catch {
        case ex: Exception =>
          log.error("Error in write to " + table_name)
          ex.printStackTrace()
      }
      log.info(s"after write cassandra, $table_name, $hash_key, $sort_key")
    }
  }

}

