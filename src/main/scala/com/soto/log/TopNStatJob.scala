package com.soto.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
/**
  * TopN统计Spark作业
  */
object TopNStatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[4]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("/home/sotowang/Downloads/datagrand_0517/clean")

//    accessDF.printSchema()
//    accessDF.show(false)

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

//    accessTopNDF.show(false)


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

    /**
      * 将统计结果写到MySQL中
      */
    try{
      accessTopNDF.foreachPartition(partitionOfRecords =>{
        val list = new ListBuffer[AccessStat]

        partitionOfRecords.foreach(info =>{
          val item_id = info.getAs[String]("item_id")
          val cart_id = info.getAs[String]("cart_id")
          val action_type = info.getAs[String]("action_type")
          val times = info.getAs[Long]("times")

          list.append(AccessStat(item_id,cart_id,action_type,times))
        })

        StatDAO.insertAccessTopN(list)


      })

    }catch {
      case e:Exception => e.printStackTrace()
    }

  }
}
