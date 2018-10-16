package com.soto

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * DataFrame 和RDD的互操作
  */
object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()


//    inferRelection(spark)

    program(spark)

    spark.stop()
  }


  def program(spark: SparkSession): Unit = {
    //RDD ==> DataFrame

    val rdd = spark.sparkContext.textFile("file:///home/sotowang/Desktop/test.txt")

    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))

    val structType = StructType(Array(StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)))


    val infoDF = spark.createDataFrame(infoRDD,structType)
    infoDF.printSchema()
    infoDF.show()

  }


    def inferRelection(spark: SparkSession): Unit = {
    //RDD ==> DataFrame

    val rdd = spark.sparkContext.textFile("file:///home/sotowang/Desktop/test.txt")


    //注意:需要导入隐式转换
    import spark.implicits._
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt))
      .toDF()

    infoDF.show()

    infoDF.filter(infoDF.col("age") > 30).show()
  }

  case class Info(id:Int, name:String, age:Int)
}
