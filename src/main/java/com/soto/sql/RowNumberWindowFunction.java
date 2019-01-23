package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * row_number() 实现分组取topN的逻辑
 */
public class RowNumberWindowFunction {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("RowNumberWindowFunction")
                .setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        HiveContext hiveContext = new HiveContext(jsc.sc());

        //创建销售额表,sales表
        hiveContext.sql("drop table  if exists sales");
        hiveContext.sql("create table if not exists sales ( " +
                " product STRING, " +
                " category STRING, " +
                "revenue BIGINT ) ");
        hiveContext.sql("load data " +
                " local inpath '/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/sales.txt' " +
                " into table sales ");

        //使用row_number()开窗函数
        //row_number()作用就是给你一份每个分组的数据 按照其排序打上一个分组内的行号
        //比如:有一个分组date=20181001,里面有3条数据,1122,1121,1124,那么对这个分组的每一行使用row_num()开窗函数以后,三等依次会获得一个组内行号
        //行号从1开始递增,比如1122 1,1121 2,1124 3

        DataFrame top3SalesDF = hiveContext.sql("" +
                "select product,category,revenue " +
                " from ( " +
                " select " +
                " product," +
                "category," +
                "revenue," +
                //row_number() 语法说明:
                //首先,在select查询时,使用row_number()函数
                //其次,row_number()后面跟上over关键字
                //然后,括号中,是partition by ,根据哪个字段进行分组
                //其次,order by 起行组内排序
                //然后,row_number()就可以给组内行一个行号
                "row_number() over (partition by category order by revenue desc ) rank " +
                "from sales " +
                " ) tmp_sales " +
                "where rank <=3 ");

        //将每组前三的数据保存到一个表中
        hiveContext.sql("drop table if exists top3_sales ");
        top3SalesDF.saveAsTable("top3_sales");




        jsc.close();
    }







}
