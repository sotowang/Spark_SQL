package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 以编程方式动态指定元数据,将RDD转换为DataFrame
 */
public class RDD2DataFrameProgrammatically {


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("RDD2DataFrameProgrammatically")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(jsc);

        //第一步,创建一个普通的RDD,但是,必须将其转换为RDD<Row>的这种格式
        final JavaRDD<String> lines = jsc.textFile("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/students.json");

        JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>() {
            public Row call(String line) throws Exception {
                String[] lineSplited = line.split(",");

                return RowFactory.create(Integer.valueOf(lineSplited[0]), lineSplited[1], Integer.valueOf(lineSplited[2]));

            }
        });

        //第二步:动态构造元数据
        //比如说,id,name等,field的名称和类型,可能都是在程序运行过程中,动态从mysql,db里
        //或者是配置文件中加载的,不固定
        //所以特别适合用这种编程方式,来构造元数据
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        //第三步,使用动态构造的元数据,将RDD转为DataFrame
        DataFrame studentDF = sqlContext.createDataFrame(studentRDD, structType);

        studentDF.registerTempTable("students");
        DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");
        List<Row> rows = teenagerDF.javaRDD().collect();

        for (Row row : rows) {
            System.out.println(row);
        }



        jsc.close();
    }







}
