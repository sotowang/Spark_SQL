package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * JSON数据源
 */
public class JSONDataSource {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("JSONDataSource")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(jsc);


        /**
         * {"name":"Leo","score": 85}
         * {"name":"Marry","score": 99}
         * {"name":"Jack","score": 74}
         */
        //针对JSON文件创建DataFrame
        DataFrame studentScoresDF = sqlContext.read().json("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/students.json");

        //针对学生成绩信息的DataFrame,注册临时表,查询分数大于80分的学生姓名和分数
        studentScoresDF.registerTempTable("student_scores");
        DataFrame goodStudentScoresDF = sqlContext.sql(
                "select name,score from student_scores where score >= 80 ");
        List<String> goodStudentNames = goodStudentScoresDF.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        //然后针对JavaRDD<String>,创建DataFrame
        List<String> studentInfoJSONS = new ArrayList<String>();
        studentInfoJSONS.add("{\"name\":\"Leo\",\"age\": 18}");
        studentInfoJSONS.add("{\"name\":\"Marry\",\"age\": 17}");
        studentInfoJSONS.add("{\"name\":\"Jack\",\"age\": 19}");

        JavaRDD<String> studentInfoJSONsRDD = jsc.parallelize(studentInfoJSONS);
        DataFrame studentInfoDF = sqlContext.read().json(studentInfoJSONsRDD);

        //针对学生基本信息DataFrame,注册临时表,然后查询分数大于80分的学生的基本信息
        studentInfoDF.registerTempTable("student_infos");


        String sql = "select name,age from student_infos where name in ( ";
        for (int i = 0; i < goodStudentNames.size(); i++) {
            sql += " '"+ goodStudentNames.get(i) + "' ";
            if (i < goodStudentNames.size() - 1) {
                sql += ",";
            }
        }

        sql += ")";

        DataFrame goodStudentInfosDF = sqlContext.sql(sql);

        //然后将两份数据的DataFrame,转换为JavaPairRDD,执行join transformation
        JavaPairRDD<String,Tuple2<Integer,Integer>> goodStudentsRDD = goodStudentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }).join( goodStudentInfosDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }));

        //然后将封装在RDD中的好学生信息,转换为RDD<Row>的形式

        JavaRDD<Row> goodStudentsRowRDD = goodStudentsRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> student) throws Exception {
                return RowFactory.create(student._1, student._2._1, student._2._2);
            }
        });

        //创建一份元数据,将JavaRDD<Row> 转换为DataFrame

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        DataFrame goodStudentsDF = sqlContext.createDataFrame(goodStudentsRowRDD, structType);

        //将好学生的全部信息保存到一个Json文件中去
        goodStudentsDF.write().format("json").save("/home/sotowang/Desktop/students/");









        jsc.close();
    }
}
