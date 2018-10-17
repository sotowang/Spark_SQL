package com.soto.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成我们的数据清洗操作
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[4]").getOrCreate()


    val accessRDD = spark.sparkContext.textFile("/home/sotowang/Downloads/datagrand_0517/output/part-00000")

//    accessRDD.take(10).foreach(println)
    //RDD => DF
//    spark.createDataFrame()
    val accessDF =  spark.createDataFrame(accessRDD.map(x=>AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    //排序
    accessDF
        .coalesce(1)
        .write.format("parquet")
        .partitionBy("cart_id")
        .mode(SaveMode.Overwrite)
        .save("/home/sotowang/Downloads/datagrand_0517/clean")
//    accessDF.printSchema()
//    accessDF.show(false)


    spark.stop()
  }
}
