package com.soto.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/**
  * TopN统计Spark作业
  */
object TopNStatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[4]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("/home/sotowang/Downloads/datagrand_0517/clean")

    accessDF.printSchema()
    accessDF.show(false)

    videoAccessTopNStat(spark,accessDF)

    spark.stop()
  }


  def videoAccessTopNStat(spark:SparkSession,accessDF:DataFrame)={
    import spark.implicits._
    val accessTopNDF = accessDF
//        .select("user_id","cart_id","item_id","action_type")
//      .filter($"cart_id" === "1_1" )
      .groupBy("item_id","cart_id","action_type").agg(count("item_id").as("times"))
      .orderBy($"times".desc)

    accessTopNDF.show(false)


//    accessDF.createOrReplaceTempView("access_logs")
//    val accessTopDF = spark.sql("select item_id,action_type,count(1) " +
//      "as times from access_logs "
////      "where cart_id='1_1'"
////      " group by item_id " +
////      "order by times desc"
//    )


//    create table item_topn(
//       item_id varchar(8) not null,
//       cart_id varchar(8) not null,
//       action_type varchar(20) not null
//       );


  }
}
