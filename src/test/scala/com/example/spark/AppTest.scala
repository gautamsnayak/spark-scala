package com.example.spark

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.FunSuite

class AppTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {

  import spark.implicits._

  def generateData = {
    val dataframe =
      Seq(("Adam", 20, "505 penobscot")).toDF("name", "age", "address")

    dataframe
  }

  test("Age under test for Adam is correct") {

    val expectedAgeDf = Seq((20)).toDF("age")

    assertDataFrameEquals(expectedAgeDf, generateData.select("age"))

    assert("20", "20")

  }

}
