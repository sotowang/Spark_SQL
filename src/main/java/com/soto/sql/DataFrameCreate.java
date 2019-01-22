package com.soto.sql;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 使用Json文件创建DataFrame
 */
public class DataFrameCreate {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("DataFrameCreate")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(jsc);


        DataFrame df =  sqlContext.read().json("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/students.json");


        //打印DataFrame中所有的数据
        df.show();


        //打印DataFrame的元数据 (Schema)
        df.printSchema();

        //查询某列所有数据
        df.select("name").show();


        //查询某几列所有数据,并对列进行计算
        df.select(df.col("name"),df.col("age").plus(1)).show();


        //根据某一列的值进行过滤

        df.filter(df.col("age").gt(18)).show();


        //根据某一列进行分组,然后进行聚合
        df.groupBy(df.col("age")).count().show();

        jsc.close();
    }
}
