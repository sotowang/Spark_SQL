package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 手动指定数据源类型
 */
public class ManuallySpecifyOptions {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("ManuallySpecifyOptions")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame usersDF = sqlContext.read()
                .format("parquet")
                .load("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/users.parquet");


        usersDF.select("name","favorite_color")
                .write()
                .format("json")
                .save("/home/sotowang/Desktop/nameAndColors");


        jsc.close();
    }
}
