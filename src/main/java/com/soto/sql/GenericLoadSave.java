package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 通用load和save操作
 */
public class GenericLoadSave {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("GenericLoadSave")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame usersDF = sqlContext.read().load("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/users.parquet");

        usersDF.printSchema();

        usersDF.show();

        usersDF.select("name","favorite_color").write().save("/home/sotowang/Desktop/nameAndColors.parquet");


        jsc.close();
    }
}
