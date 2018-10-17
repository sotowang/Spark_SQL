package com.soto

import org.apache.spark.sql.SparkSession

/**
  * schema Infer
  */
object SchemaInferApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("HiveMySQLApp")
      .master("local[2]").enableHiveSupport().getOrCreate()

    /**
      * {"name":"zhangsan","gender":"F","height":160}
      * {"name":"lisi","gender":"M","height":170,"age":30}
      * {"name":"wangwu","gender":"M","height":180.2}
      */

    val df = spark.read.format("json").load("file:///home/sotowang/Desktop/people.txt")

    df.printSchema()

    df.show()

    spark.stop()
  }
}
