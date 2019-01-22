package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class RDD2DataFrameReflection {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("RDD2DataFrameReflection")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(jsc);


        JavaRDD<String> lines = jsc.textFile("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/students.json");

        JavaRDD<Student> students = lines.map(new Function<String, Student>() {
            public Student call(String line) throws Exception {
                String[] lineSplited = line.split(",");
                Student stu = new Student();
                stu.setId(Integer.valueOf(lineSplited[0]));
                stu.setName(lineSplited[1]);
                stu.setAge(Integer.valueOf(lineSplited[2]));
                return stu;
            }
        });

        //使用反射方式将RDD转换为DataFrame
        //将Student.Class传入进去,其实就是用反射的方式来创建DataFrame
        //因为Student.class本身就是反射的一个应用
        //然后底层还得通过对Student Class进行反射,来获取其中的field
        DataFrame studentDF = sqlContext.createDataFrame(students, Student.class);

        //拿到DataFrame后,将其注册为一个临时表,然后针对其中的数据进行SQL语句
        studentDF.registerTempTable("students");


        //针对students临时表执行语句,查询年龄小于等于18岁的学生,就是teenager

        DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");

        //将查询出的DataFrame,再次转换为RDD
        JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();

        //将RDD中的数据,进行映射,映射为student
        JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(new Function<Row, Student>() {
            public Student call(Row row) throws Exception {
                Student student = new Student();
                student.setAge(row.getInt(0));
                student.setName(row.getString(2));
                student.setId(row.getInt(1));
                return student;
            }
        });


        //将数据collect,打印出来
        List<Student> studentList = teenagerStudentRDD.collect();

        for (Student student : studentList) {
            System.out.println(student.toString());
        }

        jsc.close();

    }


}
