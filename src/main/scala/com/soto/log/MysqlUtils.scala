package com.soto.log

import java.sql.{Connection, DriverManager, PreparedStatement}


/**
  * Mysql操作工具类
  */
object MysqlUtils {

  def getConnection()={

    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project?user=root&password=123456")
  }

  /**
    * 释放数据库连接资源
    * @param connection
    * @param pstmt
    */
  def release(connection:Connection,pstmt:PreparedStatement)={
    try{
      if(pstmt!= null){
        pstmt.close()
      }

    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      if(connection!= null){
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }
}
