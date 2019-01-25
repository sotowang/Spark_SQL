package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCDataSource {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("JDBCDataSource")
                .setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        //分别将mysql中两张莆中的数据加载这DataFrame
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://sotowang-pc:3306/testdb");
        options.put("user", "root");
        options.put("password", "123456");
        options.put("dbtable", "student_infos");

        DataFrame studentInfosDF = sqlContext.read().format("jdbc").options(options).load();

        options.put("dbtable", "student_scores");

        DataFrame studentScoresDF = sqlContext.read().format("jdbc").options(options).load();


        //将两个dataFrame 转换为JavaPairRDD,执行join操作
        JavaPairRDD<String,Tuple2<Integer,Integer>> studentsRDD = studentInfosDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
            }
        }).join(studentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
            }
        }));

        //将JavaPairRDD转换为 JavaRDD<Row>
        JavaRDD<Row> studentsRowRDD = studentsRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> student) throws Exception {
                return RowFactory.create(student._1, student._2._1, student._2._2);
            }
        });

        //过滤出分数大于80分的数据
        JavaRDD<Row> filterdStudentRowsRDD = studentsRowRDD.filter(new Function<Row, Boolean>() {
            public Boolean call(Row row) throws Exception {
                if (row.getInt(2) > 80) {
                    return true;
                }
                return false;
            }
        }).distinct();

        //转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        DataFrame studentDF = sqlContext.createDataFrame(filterdStudentRowsRDD, structType);


        Row[] rows = studentDF.collect();
        for (Row row : rows) {
            System.out.println(row);
        }

        options.put("dbtable", "good_student_infos");
        //将DataFrame中的数据保存到Mysql数据表中
        studentDF.javaRDD().foreach(new VoidFunction<Row>() {
            public void call(Row row) throws Exception {

                String sql = " insert into good_student_infos values('"
                        + row.getString(0) + "'," +
                        Integer.valueOf(String.valueOf(row.get(1))) + "," +
                        Integer.valueOf(String.valueOf(row.get(2))) + ")";

                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = null;
                Statement statement = null;
                try {
                    conn = DriverManager.getConnection(
                            "jdbc:mysql://127.0.0.1:3306/testdb", "root", "123456"
                    );
                    statement = conn.createStatement();
                    statement.executeUpdate(sql);
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    if (statement != null) {
                        statement.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                }
            }
        });


        jsc.close();


    }
}
