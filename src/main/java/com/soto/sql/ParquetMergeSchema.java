package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * 数据源Parquet之合并元数据(默认关闭)
 */
public class ParquetMergeSchema {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("ParquetLoadData")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(jsc);


        //创建一个DataFrame,作为学生的基本信息,并写入一个parquet文件中

        List<Tuple2<String, Integer>> studentsWithNameAge = Arrays.asList(new Tuple2<String, Integer>("leo", 23), new Tuple2<String, Integer>("Jack", 25));

        JavaRDD<Row> studentWithNameAgeRDD = jsc.parallelize(studentsWithNameAge).map(new Function<Tuple2<String, Integer>, Row>() {
            public Row call(Tuple2<String, Integer> student) throws Exception {
                return RowFactory.create(student._1, student._2);
            }
        });

        List<StructField> structFields_age = new ArrayList<StructField>();

        structFields_age.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields_age.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType_age = DataTypes.createStructType(structFields_age);

        //创建一个DataFrame,作为学生的基本信息,并写入一个parquet文件
        DataFrame studentsWithNameAgeDF = sqlContext.createDataFrame(studentWithNameAgeRDD, structType_age);
        studentsWithNameAgeDF.save("/home/sotowang/Desktop/students", SaveMode.Append);



        //创建第二个DataFrame,作为学生的成绩信息,并写入一个parquet文件中
        List<Tuple2<String, String>> studentsWithNameGrade = Arrays.asList(new Tuple2<String, String>("marry", "A"), new Tuple2<String, String>("tom", "B"));
        JavaRDD<Row> studentsWithNameGradeRDD = jsc.parallelize(studentsWithNameGrade).map(new Function<Tuple2<String, String>, Row>() {
            public Row call(Tuple2<String, String> student) throws Exception {
                return RowFactory.create(student._1, student._2);
            }
        });
        List<StructField> structFields_grade = new ArrayList<StructField>();
        structFields_grade.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields_grade.add(DataTypes.createStructField("grade", DataTypes.StringType, true));
        StructType structType_grade = DataTypes.createStructType(structFields_grade);

        //创建一个DataFrame,作为学生的基本信息,并写入一个parquet文件
        DataFrame studentsWithNameGradeDF = sqlContext.createDataFrame(studentsWithNameGradeRDD, structType_grade);
        studentsWithNameGradeDF.save("/home/sotowang/Desktop/students", SaveMode.Append);


        //第一个DataFrame和第二个DataFrame的元数据不一样,一个是包含了name和age两列,一个是包含了name和grade两列
        //这里期望读出来的表数据,自动合并含两个文件的元数据,出现三个列,name age grade

        //有mergeSchema的方式读取students表中的数据,进行元数据的合并
        DataFrame studentsDF = sqlContext.read().option("mergeSchema", "true").parquet("/home/sotowang/Desktop/students");

        studentsDF.printSchema();
        studentsDF.show();

        jsc.close();
    }
}
