# Spark SQL基础

[[北风网 Spark 2.0从入门到精通] (278讲)](https://www.bilibili.com/video/av19995678/?p=101&t=612)

* Spark SQL特点

```markdown
1. 支持多种数据源: Hive,RDD,Parquet,JSON,JDBC
2. 多种性能优化技术: in-memory columnar storage, byte-code generation, cost model动态评估等
3. 组件扩展性: 对于SQL的讲法解析器,分析器以及优化器,用户都可以自己开发,并且动态扩展

```

## Spark SQL 开发步骤 

* 创建SQLContext /HiveContext(官方推荐)对象

```java
 SparkConf sparkConf = new SparkConf()
                .setAppName("...")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(jsc);
```

## 使用Json文件创建DataFrame DataFrameCreate.java

```java
SparkConf sparkConf = new SparkConf()
                .setAppName("DataFrameCreate")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(jsc);


        DataFrame df =  sqlContext.read().json("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/students.json");

        df.show();
```







































































































































