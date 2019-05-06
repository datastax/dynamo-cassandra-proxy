package com.datastax.powertools.migrate
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.github.traviscrawford.spark.dynamodb._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
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

    println(s"entered main")
    var table_name = "na"
    var hash_key = "na"
    var sort_key = "na"
    if (args.length > 0) {
      table_name = args(0)
    } else {
      println("not enough parameters for job $args.lenght")
    }
    println(s"entered main, $table_name")

    val sparkJob = new SparkJob()
    try {
      sparkJob.runJob(table_name)
    } catch {
      case ex: Exception =>
        println("error in main running spark job")
    }
  }


  class SparkJob extends Serializable {

    println(s"before build spark session")

    def runJob(table_name: String) = {
      val appName = "DynamoReader"
      val sparkSession =
        SparkSession.builder
          .appName(appName)
          .config("spark.cassandra.connection.host", "node0")
          .getOrCreate()

      println(s"before read dynamodb, $table_name")
      val dynamoDF = sparkSession.read.dynamodb(table_name)
      dynamoDF.show(2)
      dynamoDF.printSchema()
      val keycols = getKeys(table_name)
      println(s"print key columns")
      keycols.foreach {println}
      //  hash_key is always first
      var sort_key = "na"
      if (keycols.length > 1) sort_key = keycols(1).toLowerCase
      val hash_key = keycols(0)
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
      newDF.show(2)
      newDF.printSchema()
      //  Only need to write out the three columns
      val writeDF = newDF.select(col(hash_key),col(sort_key),col("json_blob"))
      writeDF.printSchema()
      println(s"before write cassandra, $table_name, $hash_key, $sort_key")
      writeDF.write.cassandraFormat(table_name.toLowerCase, "testks").save()
      println(s"after write cassandra, $table_name, $hash_key, $sort_key")
    }
  }

}

