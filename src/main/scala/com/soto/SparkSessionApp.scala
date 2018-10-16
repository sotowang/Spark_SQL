package com.soto

import org.apache.spark.sql.SparkSession

/**
  * SparkSession的使用
  */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()

    val people = spark.read.json("/home/sotowang/user/aur/spark/spark-1.6.0-cdh5.7.0/examples/src/main/resources/people.json")

    people.show()

    spark.stop()


  }
}
