package com.soto

import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗:抽取出我们所需要的指定列的数据
  */
object SparkstatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkstatFormatJob")
      .master("local[2]").enableHiveSupport().getOrCreate()

//    val access = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/home/sotowang/Downloads/datagrand_0517/train.csv")
//
////    access.select("user_id","cart_id","item_id","action_type").save
//
//    access.write.format("csv").save("/home/sotowang/Downloads/datagrand_0517/aaa/")
//
//    access.printSchema()




//    access.select("user_id","cate_id","item_id","action_type","action_time")
//      .write.format("parquet").save("/home/sotowang/Downloads/datagrand_0517/aaa")

        val access = spark.sparkContext.textFile("/home/sotowang/Downloads/datagrand_0517/train.csv")

    //    access.take(10).foreach(println)
    access.map(line => {
      val splits = line.split(",")
      val user_id = splits(0)
      val item_id = splits(1)
      val cate_id = splits(2)
      val action_type = splits(3)
      val action_time =splits(4)

      //      (user_id,item_id,cate_id)

      user_id + "\t" + cate_id + "\t" + item_id + "\t" + action_type

    }).saveAsTextFile("/home/sotowang/Downloads/datagrand_0517/output")

    spark.stop()
  }
}
