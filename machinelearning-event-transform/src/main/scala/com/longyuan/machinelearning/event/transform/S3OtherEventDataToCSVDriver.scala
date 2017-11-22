package com.longyuan.machinelearning.event.transform

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by mulin on 2017/11/22.
  */
class S3OtherEventDataToCSVDriver {
}

object S3OtherEventDataToCSVDriver {
  def apply: S3OtherEventDataToCSVDriver = new S3OtherEventDataToCSVDriver()

  val accessKey = "AKIAOUYAW6RBUFR3ZF5A"
  val secretKey = "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"

  val bucket = "csdatas3production"
  val appId = "bc4455c1f7a9c0f7"
  val date = "2017-10-16"


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("fs.s3a.endpoint", "s3.cn-north-1.amazonaws.com.cn")
      .set("fs.s3a.access.key", accessKey)
      .set("fs.s3a.secret.key", secretKey)

    val sqlContext = SparkSession
      .builder
      .appName("SparkStreamingWriteToParquetFileExample")
      .master("local[*]")
        .config(conf)
      .getOrCreate()


//   val otherDf = sqlContext.read.load(s"s3n://$accessKey:$secretKey@$bucket/$date/other_00:00:15_1948.log", "json")

    val data = sqlContext.sparkContext.textFile(s"s3a://$bucket/$date/other_00:00:15_1948.log")

    print(s"count: ${data.count}")

  }
}
