//package com.soto
//
//import java.util.Date
//
//import org.apache.commons.lang.time.FastDateFormat
//
//
//
//object DateUtils {
//
//  def parse(time: String): String ={
//    val format =  FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
//
//    val tim =format.format(new Date(time.toLong))
//    tim
//
//  }
//
//  def main(args: Array[String]): Unit = {
//    print(parse("1486895058"))
//  }
//
//
//}
