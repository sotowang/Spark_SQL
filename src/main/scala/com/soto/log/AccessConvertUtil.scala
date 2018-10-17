package com.soto.log

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


/**
  * 访问日志转换(输入=>输出) 工具类
  */
object AccessConvertUtil {

  //定义的是输出的字段
    val struct = StructType(
      Array(
        StructField("user_id",StringType),
        StructField("cart_id",StringType),
        StructField("item_id",StringType),
        StructField("action_type",StringType)
      )
    )


  /**
    * 根据输入的每一行信息转换成输出的样式
    * @param log
    */
  def parseLog(log:String)={

    try{

      val splits = log.split("\t")
      val user_id = splits(0)
      val cart_id = splits(1)
      val item_id= splits(2)
      val action_type = splits(3)

      Row(user_id,cart_id,item_id,action_type)
    }catch{
      case e:Exception => Row(0)
    }

  }
}
