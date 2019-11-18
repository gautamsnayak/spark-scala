package com.example.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

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

    // Steps from here
    // readManifestTableAndJoinwithRequestTable
    // convertBloodCollDateToTimestamp
    // writeTransformedDataToHive
  }

  def dummyData(sparkSession: SparkSession) = {

    import sparkSession.implicits._

    val dummyData =
      Seq(("Adam", 20, "505 penobscot")).toDF("name", "age", "address")

    dummyData
  }

  def writeTransformedDataToHive(dataFrame: DataFrame) = {

    val tableName = "default.spark_scala_assignment"

  }

  /**
    * Read  bloodcolldate in u_ghemanifest which is in epoch time and convert it to timestamp
    * @param dataFrame
    * @return
    */
  def convertBloodCollDateToTimestamp(dataFrame: DataFrame) = {

    dataFrame // conversion logic here
  }

  /**
    * Read u_ghemanifest and select records only related  study A5481059.
    * Then join this with  s_request table and get final_report date column
    * from it for every record which belonging to study in u_ghemanifest.
    *
    */
  def readManifestTableAndJoinwithRequestTable() = {}

}
