package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class HiveDataSource {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("HiveDataSource")
                .setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //创建HiveContext,注意:它接收的是sparkContext为参数而不是JavaSparkContext
        HiveContext hiveContext = new HiveContext(jsc.sc());

//        hiveContext.sql("drop table emp1");
//        hiveContext.sql("CREATE TABLE IF NOT EXISTS emp1 (deptNo INT, name STRING) row format delimited fields terminated by '\\t'" +
//                "lines terminated by '\\n'" +
//                "stored as textfile");


        //第一个功能:使用HiveContext的sql()/hql() 方法,可以执行Hive中能执行的HiveQL语句

        //判断是否存在student_infos,若存在则删除
        hiveContext.sql("DROP TABLE IF EXISTS student_infos");
        //如果不存在,则创建该表
        hiveContext.sql("CREATE TABLE IF NOT EXISTS  student_infos(name STRING, age INT ) row format delimited fields terminated by ','");
        //将学生基本信息数据导入student_infos表
        hiveContext.sql("LOAD DATA " +
                " LOCAL INPATH '/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/student_infos.txt' " +
                " INTO TABLE student_infos ");

        //用同样的方式给student_scores导入数据
        hiveContext.sql("DROP TABLE IF EXISTS student_scores ");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores(name STRING, score INT ) row format delimited fields terminated by ','");
        hiveContext.sql("LOAD DATA " +
                " LOCAL INPATH '/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/student_scores.txt' " +
                " INTO TABLE student_scores ");


        //第二个功能,执行sql还可以返回DataFrame,用于查询

        //执行sql查询,关联两张表,查询成绩大于80分的学生
        DataFrame goodStudentDF = hiveContext.sql(" SELECT si.name name, si.age age, ss.score score " +
                " FROM student_infos si " +
                " JOIN student_scores ss ON si.name =ss.name " +
                " WHERE ss.score >= 80 ");


        //第三个功能,可以将DataFrame中的数据,理论上来说,DataFrame对应的RDD元素是Row即可将DataFrame中的数据保存到hive表中

        //将DataFrame中的数据保存到good_student_infos
        hiveContext.sql("DROP TABLE IF EXISTS good_student_infos ");
        goodStudentDF.saveAsTable("good_student_infos");



        //第四个功能:可以用table() 方法针对hive表,直接创建DataFrame
        //然后针对good_student_infos表直接创建DataFrame
        Row[] goodStudentsRows = hiveContext.table("good_student_infos").collect();

        for (Row row : goodStudentsRows) {
            System.out.println(row);
        }






        jsc.close();
    }
















}
