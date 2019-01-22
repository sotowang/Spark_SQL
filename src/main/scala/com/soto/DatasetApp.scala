//package com.soto
//
//import org.apache.spark.sql.SparkSession
//
//object DatasetApp {
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("DatasetApp")
//      .master("local[2]").getOrCreate()
//
//
//    //注意:需要导入隐式转换
//    import spark.implicits._
//
//    val path = "file:///home/sotowang/Desktop/test.csv"
//    //spark如何解析csv文件
//    val df = spark.read.option("header","true").option("inferSchema","true").csv(path)
//
//    df.show()
//
//    val ds = df.as[Sales]
//
//    ds.map(line => line.id).show()
//
//    spark.stop()
//  }
//
//  case class Sales(id:Int,name:String,phone:String,email:String)
//
//}
