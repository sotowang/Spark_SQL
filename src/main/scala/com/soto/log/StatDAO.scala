package com.soto.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  *  各个维度的DAO操作
  */
object StatDAO {
  /**
    * 批量保存到数据库
    * @param list
    */
  def insertAccessTopN(list:ListBuffer[AccessStat])={
    var connection:Connection = null
    var pstmt:PreparedStatement = null
    try{
      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into item_topn(item_id,cart_id,action_type,times) values (?,?,?,?)"

      pstmt = connection.prepareStatement(sql)


      for(ele <- list){
        pstmt.setString(1,ele.item_id)
        pstmt.setString(2,ele.cart_id)
        pstmt.setString(3,ele.action_type)
        pstmt.setLong(4,ele.times)
        pstmt.addBatch()
      }


      pstmt.executeBatch()
      connection.commit()  //手动捐资

    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      MysqlUtils.release(connection,pstmt)
    }
  }
}
