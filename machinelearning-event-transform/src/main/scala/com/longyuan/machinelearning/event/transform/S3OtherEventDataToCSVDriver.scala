package com.longyuan.machinelearning.event.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by mulin on 2017/11/22.
  */
class S3OtherEventDataToCSVDriver {
}

object S3OtherEventDataToCSVDriver {
  def apply: S3OtherEventDataToCSVDriver = new S3OtherEventDataToCSVDriver()

  val accessKey = "AKIAO4RERNKGOMSUNZVA"
  val secretKey = "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"

  val bucket = "csdatas3production"
  val appId = "bc4455c1f7a9c0f7"
  val date = "2017-10-16"


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.5");

    val conf = new SparkConf()
        .setAppName("SparkStreamingWriteToParquetFileExample")
        .setMaster("local[*]")

    val sqlContext = SparkSession
      .builder
      .appName("SparkStreamingWriteToParquetFileExample")
      .master("local[*]")
        .config(conf)
      .getOrCreate()
    val sc = sqlContext.sparkContext

//    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.cn-north-1.amazonaws.com.cn")

//   val otherDf = sqlContext.read.load(s"s3n://$accessKey:$secretKey@$bucket/$date/other_00:00:15_1948.log", "json")

//    val data = sc.textFile(s"s3a://$accessKey:$secretKey@$bucket/$appId/$date")
//val data = sc.textFile(s"s3a://$bucket/$appId/$date/exit*")
//
//    val data = sc.textFile("s3a://csdatas3production/234/wangsen")
//    println(s"count: ${data.count}")

    val otherDf = sqlContext.read.json("s3a://csdatas3production/234/wangsen")
    otherDf.printSchema()
    otherDf.show(10)

  }
}
