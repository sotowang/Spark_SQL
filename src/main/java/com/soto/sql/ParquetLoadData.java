package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * 数据源Parquet之使用编程方式加载数据
 */
public class ParquetLoadData {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("ParquetLoadData")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(jsc);


        DataFrame usersDF = sqlContext.read().parquet("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/users.parquet");

        //将DataFrame注册为临时表,使用SQL查询需要的数据
        usersDF.registerTempTable("users");

        DataFrame userNameDf = sqlContext.sql("select name from users ");

        //对查询出的DataFrame进行transformation操作,处理数据,然后打印
        List<String> userNames = userNameDf.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) throws Exception {
                return "Name: " + row.getString(0);
            }
        }).collect();

        for (String userName : userNames) {
            System.out.println(userName);
        }



        jsc.close();
    }


}
