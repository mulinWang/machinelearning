package com.longyuan.machinelearning.event.transform

import java.io.BufferedInputStream
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by mulin on 2017/11/27.
  */
object S3LoginEventDataToCSVDriver {
  val properties: Properties = new Properties

  val loadProperties: Unit  = {
    try {
      val in = getClass.getResourceAsStream("/driver.properties")
      properties.load(new BufferedInputStream(in))
    }catch {
      case e: Exception => println(s"load properties exception: ${e}" )
    }
  }

  val master = properties.getProperty("driver.master", "local[*]")


  val accessKey = "AKIAO4RERNKGOMSUNZVA"
  val secretKey = "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"

  val bucket = "csdatas3production"
  val appId = "bc4455c1f7a9c0f7"

  def main(args: Array[String]): Unit = {
    var date = "2017-08-01"
    var files = "login_*.log"

    if (null != args && args.length > 0) {
      date = args(0)
      if (args.length == 2) {
        files = args(1)
      }
    }
    val conf = {
      val tempConf = new SparkConf()
        .setAppName("SparkStreamingWriteToParquetFileExample")
      if ("local[*]".equals(master)) {
        tempConf.setMaster(master)
      }
      tempConf
    }


    val sqlContext = SparkSession
      .builder
      .config(conf)
      .getOrCreate()
    val sc = sqlContext.sparkContext

    sc.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.cn-north-1.amazonaws.com.cn")

    val path = s"s3a://csdatas3production/bc4455c1f7a9c0f7/$date/$files"

    println(s"date: $date")
    println(s"file: $files")
    println(s"path: $path")

    val loginDF = sqlContext.read.json(path)


    import sqlContext.implicits._

    val newLoginDF = loginDF.toJSON.rdd.map(line => {
      try {
        val json = JSON.parseObject(line)
        json
      }catch {
        case ex: Exception => {
          println(s"exception: $ex")
        }
          null
      }
    }).filter(json => null != json &&
      "login".equals(json.getString("event"))
    ).map(json => {
      val accountId = json.getString("accountId")
      val ts = json.getString("ts")
      (accountId, ts)
    }).filter(x => (null != x._1 && null != x._2))
        .toDF()

    newLoginDF.printSchema()
    newLoginDF.show(10)

    newLoginDF.write.csv(s"file:///home/hadoop/data/loginEvent/$date/")
//    newLoginDF.repartition(1).write.csv(s"E:\\projects\\data\\$date")


    sqlContext.stop()
    println("job finish!!!")
  }
}

//case class Login(accountId: String, ts: String)