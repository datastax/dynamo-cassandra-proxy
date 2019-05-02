package com.datastax.powertools.migrate
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.github.traviscrawford.spark.dynamodb._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._


object dynamoDB {


  def main(args: Array[String]) {

    println(s"entered main")
    var table_name = "na"
    var hash_key = "na"
    var sort_key = "na"
    if (args.length > 1) {
      table_name = args(0)
      hash_key = args(1).toLowerCase
      if (args.length > 2) sort_key = args(2).toLowerCase
    } else {
      println("not enough parameters for job")
    }
    println(s"entered main, $table_name, $hash_key, $sort_key")

    val sparkJob = new SparkJob()
    try {
      sparkJob.runJob(table_name, hash_key, sort_key)
    } catch {
      case ex: Exception =>
        println("error in main running spark job")
    }
  }


  class SparkJob extends Serializable {

    println(s"before build spark session")

    def runJob(table_name: String, hash_key: String, sort_key: String) = {
      val appName = "DynamoReader"
      val sparkSession =
        SparkSession.builder
          .appName(appName)
          .config("spark.cassandra.connection.host", "node0")
          .getOrCreate()

      println(s"before read dynamodb, $table_name, $hash_key, $sort_key")
      val dynamoDF = sparkSession.read.dynamodb(table_name)
      dynamoDF.show(2)
      dynamoDF.printSchema()
      //  gets all columns labels into a list, this will be used for list of json columns
      val cols = dynamoDF.columns.toSeq
     // remove the hash_key and the sort_key as they should not be in json string
      val othercols = cols.filterNot(x => x == hash_key).filterNot(x => x == sort_key)
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

