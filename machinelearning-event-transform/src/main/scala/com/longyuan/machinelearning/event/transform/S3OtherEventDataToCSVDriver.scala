package com.longyuan.machinelearning.event.transform

import com.alibaba.fastjson.{JSON}
import org.apache.spark.{SparkConf}
import org.apache.spark.sql.SparkSession

/**
  * Created by mulin on 2017/11/22.
  */

object S3OtherEventDataToCSVDriver {

  val accessKey = "AKIAO4RERNKGOMSUNZVA"
  val secretKey = "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"

  val bucket = "csdatas3production"
  val appId = "bc4455c1f7a9c0f7"

  def main(args: Array[String]): Unit = {
    var date = "2017-10-16"
    var files = "other*.log"

    if (null != args && args.length > 0) {
      date = args(0)
      if (args.length == 2) {
        files = args(1)
      }

    }
    val conf = new SparkConf()
        .setAppName("SparkStreamingWriteToParquetFileExample")
        .setMaster("local[*]")

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

    val otherDF = sqlContext.read.json(path)

    import sqlContext.implicits._

    val newOtherDF = otherDF.toJSON.rdd.map(line => {
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
      "other".equals(json.getString("event")) &&
      {
        val dataJson = json.getJSONObject("data")
        (null != dataJson.get("incr") || null != dataJson.get("decr"))
      })
      .map(json => {
        //事件名
        val otherEvent = json.getString("otherEvent")
        //时间戳
        val ts = json.getString("ts")
        //账号名
        val accountId = json.getString("accountId")
        //json data
        val dataJson = json.getJSONObject("data")
        //场景
        val locale = dataJson.getOrDefault("locale", "").toString
        //获取渠道？
        val via = dataJson.getOrDefault("via", "").toString

        val incr = dataJson.getJSONObject("incr")
        val decr = dataJson.getJSONObject("decr")

        var Gem = 0
        var Gold = 0
        var Steel = 0
        var Food = 0
        var Lumber = 0
        var Marble = 0
        var Crystal = 0

        if (null != incr) {
          Gem = incr.getIntValue("Gem")

          Gold = incr.getIntValue("Gold")

          Steel = incr.getIntValue("Steel")

          Food = incr.getIntValue("Food")

          Lumber = incr.getIntValue("Lumber")

          Marble = incr.getIntValue("Marble")

          Crystal = incr.getIntValue("Crystal")

          Other(otherEvent, ts, accountId, locale, via, Gem, Gold, Steel, Food, Lumber, Marble, Crystal)
        }else if(null != decr) {
          Gem = decr.getIntValue("Gem")

          Gold = decr.getIntValue("Gold")

          Steel = decr.getIntValue("Steel")

          Food = decr.getIntValue("Food")

          Lumber = decr.getIntValue("Lumber")

          Marble = decr.getIntValue("Marble")

          Crystal = decr.getIntValue("Crystal")

          Other(otherEvent, ts, accountId, locale, via, -Gem, -Gold, -Steel, -Food, -Lumber, -Marble, -Crystal)
        }else {
          println(s"exception json: $json")
          null
        }
      }).filter(other => null != other).toDF()

    newOtherDF.printSchema()
    newOtherDF.show(10)

    newOtherDF.write.csv(s"file:///home/hadoop/data/$date/other.csv")
    newOtherDF.write.csv(s"s3a://csdatas3production/234/wangsen/other.csv")
  }
}

case class Other(otherEvent: String, ts: String, accountId: String, locale: String,
                 via: String, Gem: Int, Gold: Int, Steel: Int, Food: Int, Lumber: Int, Marble: Int, Crystal: Int) {
}