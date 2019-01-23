package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.countDistinct;

public class DailyUV1 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("DailyUV1")
                .setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        List<String> userAccessLog = Arrays.asList(
                "2015-10-01,1122",
                "2015-10-01,1122",
                "2015-10-01,1123",
                "2015-10-01,1124",
                "2015-10-01,1124",
                "2015-10-02,1122",
                "2015-10-02,1121",
                "2015-10-02,1123",
                "2015-10-02,1123"
        );

        JavaRDD<String> userAccessLogRDD = jsc.parallelize(userAccessLog,5);

        //将模拟出来的用户访问日志RDD,转换为DataFrame
        //首先,将普通的RDD,转换为元素为Row的RDD
        JavaRDD<Row> userAccessLogRowRDD = userAccessLogRDD.map(new Function<String, Row>() {
            public Row call(String line) throws Exception {
                String[] logSplited = line.split(",");
                return RowFactory.create(logSplited[0], Integer.valueOf(logSplited[1]));
            }
        });


        ArrayList<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("userid", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        //创建DataFrame
        DataFrame userAccessLogRowDF = sqlContext.createDataFrame(userAccessLogRowRDD, structType);



        //使用Spark新版本内置函数

        //每天都有很多用户来访问,但每个用户每天都会访问很多次
        //uv指:对用户进行去重以后的访问总数
        //聚合函数用法:首先对DataFrame调用groupBy()方法,对某一列进行分组,然后调用agg()方法,对数为内置函数,对见其源码
        JavaRDD<Row> userAccessLogDistinctedRowRDD = userAccessLogRowDF.groupBy("date")
                .agg(countDistinct("userid"))  //(date,count(userid))
                .javaRDD();

//        userAccessLogDistinctedRowRDD.collect();

        userAccessLogDistinctedRowRDD.foreach(new VoidFunction<Row>() {
            public void call(Row row) throws Exception {
                System.out.println(row);
            }
        });

        jsc.close();
    }
}
