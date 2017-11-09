package com.longyuan.machinelearning.test.spark

import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
  * Created by mulin on 2017/11/9.
  */
class LinearSupportVectorMachineExample extends FunSuite {
  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.5")

  lazy val spark = SparkSession.builder.master("local[*]").appName("LinearSupportVectorMachineExample").getOrCreate()

  test("linear support vector machine") {
    val dataPath = getClass.getResource("/data/sample_libsvm_data.txt").getPath
    // Load and parse the data file, converting it to a DataFrame.
    val data = spark.read.format("libsvm").load(dataPath)

    val lsvc = new LinearSVC()
      .setMaxIter(10)
      .setRegParam(0.1)

    // Fit the model
    val lsvcModel = lsvc.fit(data)

    // Print the coefficients and intercept for linear svc
    println(s"Coefficients: ${lsvcModel.coefficients} Intercept: ${lsvcModel.intercept}")
  }
}
