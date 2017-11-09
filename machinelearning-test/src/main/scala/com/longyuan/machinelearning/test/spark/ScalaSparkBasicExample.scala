package com.longyuan.machinelearning.test.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

/**
  * Created by mulin on 2017/11/8.
  */
class ScalaSparkBasicExample extends FunSuite {

  /**
    * 初始化Spark Context实例，lazy 表示懒加载
    */
  lazy val spark = SparkSession
    .builder
    .appName("ScalaSparkBasicExample")
    .master("local[*]")
    .getOrCreate()
  /**
    * 测试 map函数
    * map是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD。 任何原RDD中的元素在新RDD中都有且只有一个元素与之对应。
    */
  test("map test") {

    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6), 2)
    val result = rdd.map(f => f * 2)
    println(s"[测试 map函数]MAP返回结果:${result.count}")
    result.foreach(id => println(s"转换结果：$id"))
    println("===========================================================")
  }

  /**
    * 测试mapValues函数
    * mapValues顾名思义就是输入函数应用于RDD中Kev-Value的Value，原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素。
    * 因此，该函数只适用于元素为KV对的RDD。
    */
  test("mapValues test") {

    val list: Array[(Int, Int)] = Array((1, 3), (1, 2), (1, 4), (2, 3))

    val rdd = spark.sparkContext.parallelize(list)
    println(s"分片数：${rdd.partitions.size}")

    val result = rdd.mapValues(v => v * 2)

    println(s"[测试mapValues函数]返回结果: ${
      result.map(t => {
        (t._1 + "=" + t._2 + ", ")
      }).collect
        .mkString
    }");

    println("===========================================================");
  }

  /**
    * 测试 flatMap函数
    * 与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素来构建新RDD。
    * 举例：对原RDD中的每个元素x产生y个元素（从0 到y，y为元素x的值）
    * 简单来说就是 自函子范畴上的一个协变函子的态射函数与自然变换的组合
    */
  test("flatMap test"){
    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4), 2)
    val result = rdd.flatMap(t => {
      val list = ArrayBuffer.empty[Int]
      for ( i <- 0 until t) {
        list += i
      }
      list.iterator
    })

    println(s"[测试flatMap函数]返回条数: ${result.count}")
    println(s"[测试flatMap函数]返回结果: ${result.map(itr => s"$itr ,").collect().mkString}")
  }

    /**
    * 测试mapPartition函数
    * mapPartitions是map的一个变种。map的输入函数是应用于RDD中每个元素，
    * 而mapPartitions的输入函数是应用于每个分区，也就是把每个分区中的内容作为整体来处理的。
    */
  test("mapPartitions test")  {
    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    println(s"rdd分区大小:${rdd.partitions.size}")

    val result = rdd.mapPartitions(t => {
      val list = ArrayBuffer.empty[Int]
      var a = 0
      while (t.hasNext){ a += t.next() }
      list += a
      list.iterator
    })

    println(s"[测试mapPartition函数]mapPartition返回条数结果，即rdd分区大小: ${result.count}")

    result.foreach(itr => println(s"分区累加的总数值：${itr}"))
  }

  /** *
    * 测试mapPartitionsWithIndex函数
    * mapPartitionsWithIndex是函数作用同mapPartitions，不过提供了两个参数，第一个参数为分区的索引。
    */
  test("mapPartitionsWithIndex test"){
    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5), 2)
    println(s"rdd分区大小:${rdd.partitions.size}")

    val result = rdd.mapPartitionsWithIndex((index, arr) => {
      val list = ArrayBuffer.empty[String]
      var a = ""
      while (arr.hasNext) { a += (arr.next + ";") }
      list += (s"[$index |${a}] ")
      list.iterator
    }, false)

    println(s"[测试mapPartitionsWithIndex函数]返回条数，即分区数: ${result.count}")
    println(s"[测试mapPartitionsWithIndex函数]返回结果: ${result.collect.mkString(",")}")
  }

  /**
    * aggregate函数将每个分区里面的元素进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
    * 这个函数最终返回的类型不需要和RDD中元素类型一致。
    * 执行步骤：
    * 1: 零值11参与 seqOp 方法的执行，
    * 2: seqOp 方法执行完成之后，零值11继续参与 combOp方法的执行
    * 3: 具体执行步骤 如下:
    * seqOp:11,1
    * seqOp:1,2
    * seqOp:1,3
    * combOp:11,1
    * seqOp:11,4
    * seqOp:4,5
    * seqOp:4,6
    * combOp:12,4
    * 聚合返回结果:16
    */
  test("aggregate test") {
    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6), 2)
    val result = rdd.aggregate(11)((a, b) => {
      println(s"seqOp: $a, $b")
      Math.min(a, b)
    }, (a, b) => {
      println(s"combOp: $a, $b")
       a + b
    })

    println(s"[测试 aggregate 函数]返回结果: $result")
  }

  /**
    * reduce将RDD中元素两两传递给输入函数，同时产生一个新的值，新产生的值与RDD中下一个元素再被传递给输入函数直到最后只有一个值为止。
    */
  test("reduce test") {
    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6), 2)
    // 测试reduce函数
    val result = rdd.reduce((m, n) => m + n)

    println(s"[测试 reduce 函数]返回结果: $result")
  }

  /**
    * fold和reduce的原理相同，但是与reduce不同，相当于每个reduce时，迭代器取的第一个元素是zeroValue
    */
  test("fold test") {
    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4))
    val result = rdd.fold(3)((a, b) => {
      println(s"$a + $b = ${(a + b)}")
      a + b
    })
    println(s"[测试 fold 函数]返回结果: $result")
  }

  /**
    * 顾名思义，reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行reduce，因此，Key相同的多个元素的值被reduce为一个值，然后与原RDD中的Key组成一个新的KV对。
    */
  test("reduceByKey test") {
    val list: Array[(Int, Int)] = Array((1, 3), (1, 2), (2, 4), (2, 3))

    // 测试reduceByKey函数
    val rdd = spark.sparkContext.parallelize(list)
    val result = rdd.reduceByKey((m, n) => m + n)

    println(s"[测试 reduceByKey函数]返回结果: ${result.map(t => {s"${t._1}=${t._2}"}).collect.mkString(",")}")
  }

  /**
    * zip 拉链函数
    * [测试 zip 函数]返回结果:[(1,a), (2,b), (3,c), (4,d), (5,e), (6,f), (7,g), (8,h), (9,i), (10,j)]
    */
  test("zip test"){
    val rdd1 = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val rdd2 = spark.sparkContext.parallelize(Array("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"))
    val result = rdd1.zip(rdd2)
    println(s"[测试 zip 函数]返回结果: ${result.collect.mkString(", ")}")
  }

  /**
    * glom 函数将每一个分区设置成一个数组
    * [测试 glom函数]返回结果:[[1, 2, 3], [4, 5, 6]]
    */
  test("glom test") {
    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6), 2)
    val result = rdd.glom
    println(s"[测试 glom函数]返回结果: ${result.map(arr => s"${arr.mkString("[", ", ", "]")} ").collect.mkString(",")}")
  }

  /**
    * cartesian就是笛卡尔积运算
    * [测试 cartesian函数]返回结果:[(1,4), (1,5), (1,6), (2,4), (2,5), (2,6), (3,4), (3,5), (3,6)]
    */
  test("cartesian test") {
    // 测试cartesian函数
    val rdd1 = spark.sparkContext.parallelize(Array(1, 2, 3))
    val rdd2 = spark.sparkContext.parallelize(Array(4, 5, 6))
    val result = rdd1.cartesian(rdd2)
    println(s"[测试 cartesian函数]返回结果: ${result.collect.mkString(",")}")
  }

  /**
    * intersection 取得两个RDD的交集
    * [测试 intersection 函数]返回结果:[8, 9, 10, 6, 7]
    */
  test("intersection test") {
    val rdd1 = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val rdd2 = spark.sparkContext.parallelize(Array(6, 7, 8, 9, 10, 11))
    val result = rdd1.intersection(rdd2)
    println(s"[测试 intersection 函数]返回结果: ${result.collect.mkString(", ")}")
  }

  /**
    * union 取得两个RDD的交集
    * [测试 union 函数]返回结果:[ 1, 2, 3, 4, 5, 6, 6, 7, 8, 9, 10, 11]
    */
  test("union test") {
    val rdd1 = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6))
    val rdd2 = spark.sparkContext.parallelize(Array(6, 7, 8, 9, 10, 11))
    val result = rdd1.union(rdd2)
    println(s"[测试 union 函数]返回结果: ${result.collect.mkString(", ")}")
  }

  /**
    * subtract 函数  对两个RDD中的集合进行差运算
    * [测试 subtract 函数]返回结果:[2, 4, 1, 3]
    */
  test("subtract test") {
    val rdd1 = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6), 2)
    val rdd2 = spark.sparkContext.parallelize(Array(5, 6), 2)
    val result = rdd1.subtract(rdd2, 4) // 第二个参数表示结果RDD的分区数
    println(s"[测试 subtract 函数]返回结果： ${result.collect.mkString(",")}")
    println(s"[测试 subtract 函数]返回分区数: ${result.partitions.size}")
  }

  /**
    * sample 采样函数
    * [测试 sample 函数]返回结果:[2, 4, 7, 9]
    * [测试 takeSample 函数]返回结果: 4 1 2
    */
  test("sample test") {
    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    // true 表示有放回去的抽样
    // false 表示没有放回去的抽样
    // 第二个参数为采样率 在 0->1 之间
    val result = rdd.sample(false, 0.4)
    println(s"[测试 sample 函数]返回结果: ${result.collect.mkString(", ")}")
    // 第一个参数和sample函数是相同的，第二个参数表示采样的个数
    val result1 = rdd.takeSample(false, 3)
    println(s"[测试 takeSample 函数]返回结果: ${result1.mkString(", ")}")
  }

  /**
    * combineByKey 函数测试
    * [combineByKey测试]返回结果:{2=[5, 3], 1=[4, 2, 3]}
    */
  test("combineByKey test") {
    val list = Array((1, 3), (1, 2), (1, 4), (2, 3), (2, 3), (2, 5))

    val rdd = spark.sparkContext.parallelize(list)
    val result = rdd.combineByKey(i => {
      val arr = ArrayBuffer.empty[Int]
      arr += i
      arr
    }, (arr: ArrayBuffer[Int], i) => {
      arr += i
      arr
    }, (arr1: ArrayBuffer[Int], arr2: ArrayBuffer[Int]) => {
      arr1 ++= arr2
      arr1
    })

    println(s"[combineByKey测试]返回结果: ${result.collect.mkString(", ")}")
  }
}
