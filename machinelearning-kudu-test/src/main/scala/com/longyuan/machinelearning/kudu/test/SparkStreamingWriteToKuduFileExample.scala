package com.longyuan.machinelearning.kudu.test

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.apache.kudu.spark.kudu._

import scala.collection.JavaConverters._

/**
  * Created by mulin on 2017/11/16.
  */
class SparkStreamingWriteToKuduFileExample  extends FunSuite{
  lazy val sqlContext = SparkSession
    .builder
    .appName("SparkStreamingWriteToParquetFileExample")
    .master("local[*]")
    .getOrCreate()

  // Use KuduContext to create, delete, or write to Kudu tables
  val kuduContext = new KuduContext("172.16.249.38:7051", sqlContext.sparkContext)
  case class Person(name: String, age: Long)

  val schemaString = "id name age"

  val schema = StructType(
    schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, true))
  )

  val tableName = "test_tables";
  test("test spark streaming write to kudu file") {
    //createTable(tableName);
    //deleteTable("test_table");

    //    val rdd = sqlContext.sparkContext.parallelize(Seq(
    //      Row("1", "First Value", "10"),
    //      Row("2", "Second Value", "12")
    //    ))
    //    val df = sqlContext.createDataFrame(rdd, schema)
    //    insertRows(df,tableName);

    val df = readTable(tableName);
    df.show();
  }

  def createTable (tableName:String): Unit = {
    println("start test create");
    kuduContext.createTable(
      tableName, schema, Seq("id"),
      new CreateTableOptions()
        .setNumReplicas(1)
        .addHashPartitions(List("id").asJava,3))
    println("end test create");
  }

  def deleteTable (tableName:String): Unit = {
    kuduContext.deleteTable(tableName);
  }

  def readTable (tableName:String) : DataFrame = {
    sqlContext.read.options(Map("kudu.master" -> "172.16.249.38:7051","kudu.table" -> tableName)).kudu
  }

  def insertRows (df : DataFrame,tableName:String): Unit = {
    kuduContext.insertRows(df, tableName);
  }
}
