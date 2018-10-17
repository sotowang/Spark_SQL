package com.soto

import org.apache.spark.sql.SparkSession

/**
  * Parquet文件操作
  */
object ParquetApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()


    val path = "file:///home/sotowang/Desktop/part-r-00005.gz.parquet"

    val userDF = spark.read.format("parquet").load(path)

    userDF.printSchema()
    userDF.show()

    userDF.select("name").write.format("json").save("file:///home/sotowang/Desktop/aaa.json")


    /*

    val jdbc = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/hive").option("dbtable","hive.TBLS")
      .option("user","root").option("password","123456").option("driver","com.mysql.jdbc.Driver").load()
     */

    spark.stop()
  }
}
