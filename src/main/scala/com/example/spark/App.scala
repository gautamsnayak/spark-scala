package com.example.spark

import org.apache.spark.sql.SparkSession

/**
  * Hello world!
  *
  */
object App {

  def main(args: Array[String]): Unit = {

    println("Hello Spark and Scala!")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    val data = dummyData(sparkSession)

    data.show(false)

    data.select("address").show(false)
  }

  def dummyData(sparkSession: SparkSession) = {

    import sparkSession.implicits._

    val dummyData =
      Seq(("Adam", 20, "505 penobscot")).toDF("name", "age", "address")

    dummyData
  }
}
