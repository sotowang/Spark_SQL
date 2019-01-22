//package com.soto
//
//import org.apache.spark.sql.{Row, SparkSession}
//
///**
//  * 使用外部数据源综合查询Hie和MySQL的表数据
//  */
//object HiveMySQLApp {
//
//  case class Record(key: Int, value: String)
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("HiveMySQLApp")
//      .master("local[2]").enableHiveSupport().getOrCreate()
//
//    import spark.implicits._
//    import spark.sql
//    sql("drop table emp")
//    sql("CREATE TABLE IF NOT EXISTS emp (deptNo INT, name STRING) row format delimited fields terminated by '\\t'" +
//      "lines terminated by '\\n'" +
//      "stored as textfile")
//    sql("LOAD DATA LOCAL INPATH 'file:///home/sotowang/Desktop/people.txt' overwrite INTO TABLE emp")
////
////    sql("SELECT * FROM src").show()
////
////    sql("SELECT COUNT(*) FROM src").show()
////
////    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
////
////    sqlDF.show()
////
////    val stringsDS = sqlDF.map {
////      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
////    }
////    stringsDS.show()
//
////
//    //加载Hive表数据
//
//    val hiveDF = spark.table("emp")
//
//    val mysqlDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306").option("dbtable", "spark.DEPT")
//      .option("user", "root").option("password", "123456").option("driver", "com.mysql.jdbc.Driver").load()
//
//
//    //JOIN操作
//    val resultDF = hiveDF.join(mysqlDF, hiveDF.col("deptNo") === mysqlDF.col("DEPTNO"))
//
//    resultDF.show()
//
////    resultDF.select(hiveDF.col("empno"),hiveDF.col("ename"),
////      mysqlDF.col("deptno"),mysqlDF.col("dname")).show()
//
//    spark.stop()
//
//  }
//}
