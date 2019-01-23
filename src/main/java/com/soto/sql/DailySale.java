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
import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DailySale {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("DailSale")
                .setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        List<String> userSaleLog = Arrays.asList(
                "2015-10-01,55.05,1122",
                "2015-10-01,15.20,",
                "2015-10-01,55.50,1155",
                "2015-10-02,33.22,1124",
                "2015-10-02,15.84,1124",
                "2015-10-02,15.52,1124",
                "2015-10-02,74.25,1124"
        );

        JavaRDD<String> userSaleLogRDD = jsc.parallelize(userSaleLog, 5);

        //进行有效销售日志的过滤
        JavaRDD<String> fiteredSaleLogRDD = userSaleLogRDD.filter(new Function<String, Boolean>() {
            public Boolean call(String log) throws Exception {
                if (log.split(",").length == 3) {
                    return true;
                }
                return false;
            }
        });


        JavaRDD<Row> userSaleRowRDD = fiteredSaleLogRDD.map(new Function<String, Row>() {
            public Row call(String s) throws Exception {
                String[] splitedString = s.split(",");
                return RowFactory.create(splitedString[0], Double.valueOf(splitedString[1]));
            }
        });


        ArrayList<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("sale", DataTypes.DoubleType, true));

        StructType structType = DataTypes.createStructType(structFields);

        //创建DataFrame
        DataFrame userSaleLogDF = sqlContext.createDataFrame(userSaleRowRDD, structType);

        //每日销售额统计
        JavaRDD<Row> userSaleLogSumRowRDD = userSaleLogDF.groupBy("date")
                .agg(sum("sale"))
                .javaRDD();

        userSaleLogSumRowRDD.foreach(new VoidFunction<Row>() {
            public void call(Row row) throws Exception {
                System.out.println(row);

            }
        });

        jsc.close();
    }

}
