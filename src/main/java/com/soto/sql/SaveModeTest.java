package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class SaveModeTest {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("ManuallySpecifyOptions")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame usersDF = sqlContext.read()
                .format("parquet")
                .load("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/users.parquet");
//        usersDF.save("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/users.parquet", SaveMode.ErrorIfExists);
//        usersDF.save("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/", SaveMode.Append);


        jsc.close();
    }
}
