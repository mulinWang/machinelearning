package com.longyuan.machinelearning.parquet.test

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import org.json4s.native.JsonMethods
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  * Created by mulin on 2017/11/16.
  */
class SparkStreamingWriteToParquetFileExample extends FunSuite {
  val logger = LoggerFactory.getLogger(getClass)

  val sqlContext = SparkSession
    .builder
    .appName("SparkStreamingWriteToParquetFileExample")
    .master("local[*]")
    .getOrCreate()

  var newContextCreated = false      // Flag to detect whether new context was created or not

  val stopActiveContext = true
  // "true"  = stop if any existing StreamingContext is running;
  // "false" = dont stop, and let it run undisturbed, but your latest code may not be used

  val eventsPerSecond = 1000    // For the dummy source

  val batchIntervalSeconds = 10

  val outputDirectory = "E:\\projects\\machinelearning"
  // Consolidate data for the last 60 seconds and write them out in one shot. You could have chosen a batch interval of 60 seconds too. If your streaming job needs to do some other processing at a lower granularity of time, then you can have a smaller batch interval and then use the window and sliding interval to write only every N seconds to ensure that there is less write overhead and also only less # of files are written.
  val writeEveryNSeconds = 60
  // # of files that needs to be written out for every batch (writeEveryNSeconds).
  val numberOfFilesPerBatch = 2

  // Function to create a new StreamingContext and set it up
  def streamingContext(): StreamingContext = {
    // Create a StreamingContext
    val ssc = new StreamingContext(sqlContext.sparkContext, Seconds(batchIntervalSeconds))

    // Create a stream that generates 1000 lines per second
    println("Creating function called to create new StreamingContext")
    newContextCreated = true
    ssc
  }

  /**
    * spark streaming 写 parquet 实践 1
    */
  test("test spark streaming write to parquet file ex1") {
    if (stopActiveContext) {
      StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
    }

    // Get or create a streaming context
    val ssc = StreamingContext.getActiveOrCreate(streamingContext)

    if (newContextCreated) {
      println("New context created from currently defined creating function")
    } else {
      println("Existing context running or recovered from checkpoint, may not be running currently defined creating function")
    }

    val stream = ssc.receiverStream(new CustomReceiver(eventsPerSecond))

    val schemaString = (1 to 100).map(i => "C" + i).mkString(",")
    val schema = StructType(
      schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true))
    )

    println(s"schema: ${schema.mkString}")

    // Split the event into fields and print the # of events
    val rowStream = stream.map(event => Row.fromSeq(event.split(",")))

    rowStream.print(10)
    val sdf = new SimpleDateFormat("yyyyMMddhh")

    // Write to S3 in Parquet format for every N seconds.
    rowStream.window(Seconds(writeEveryNSeconds), Seconds(writeEveryNSeconds)).foreachRDD {
      (rdd, time) =>
        val df = sqlContext.createDataFrame(rdd.coalesce(numberOfFilesPerBatch), schema)
        val datehour = sdf.format(new Date(time.milliseconds))    // UTC time as shown in Spark Streaming UI.
      val finalDf = df.withColumn("datehr", lit(datehour))
        //        finalDf.write.mode(SaveMode.Append).partitionBy("datehr").save(outputDirectory)
        finalDf.write.mode(SaveMode.Append).save(outputDirectory)
    }

    ssc.remember(Minutes(1))

    // Start the streaming context in the background.
    ssc.start()

    // This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 2 times the batchIntervalSeconds.
    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 2 * 1000 * 60)
  }

  /**
    * spark streaming 写 parquet 实践 2
    */
  test("test spark streaming write to parquet file ex2") {


    if (stopActiveContext) {
      StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
    }

    // Get or create a streaming context
    val ssc = StreamingContext.getActiveOrCreate(streamingContext)

    if (newContextCreated) {
      println("New context created from currently defined creating function")
    } else {
      println("Existing context running or recovered from checkpoint, may not be running currently defined creating function")
    }
    val stream = ssc.receiverStream(new CustomJsonReceiver(eventsPerSecond))

    stream.map(str => {
      import org.json4s._
      implicit val formats = DefaultFormats

      val loginReqParse = JsonMethods.parse(str)
      val loginReq = loginReqParse.extract[LoginReq]
      loginReq
    }).foreachRDD(rdd => {
      import sqlContext.implicits._
      val loginReqDF = rdd.toDF

      loginReqDF.show(5)
      loginReqDF.write.mode(SaveMode.Append).parquet("loginReqDFJava.parquet")
    })

    // Start the streaming context in the background.
    ssc.start()

    // This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 2 times the batchIntervalSeconds.
    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 2 * 1000 * 60)
  }

}

class CustomReceiver(ratePerSec: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def generateData(): String = {
    (1 to 100).map(i => "Random" + i + Random.nextInt(100000)).mkString(",")
  }

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Dummy Source") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    while(!isStopped()) {
      store(generateData())
      Thread.sleep((1000.toDouble / ratePerSec).toInt)
    }
  }
}

class CustomJsonReceiver(ratePerSec: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def generateData(): String = {
    s"""{ "Udid": "${Random.nextString(5)}",
           "WorldId": "${Random.nextString(3)}",
           "Platform": ${Random.nextInt(2)},
           "DeviceId": "${Random.nextString(3)}",
           "ChannelId": "${Random.nextString(3)}"
           }"""
  }

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Dummy Source") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    while(!isStopped()) {
      store(generateData())
      Thread.sleep((1000.toDouble / ratePerSec).toInt)
    }
  }
}


case class LoginReq(Udid: String, WorldId: String, Platform: Int, DeviceId: String, ChannelId: String)