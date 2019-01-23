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

### DataFrame 常用操作

打印DataFrame中所有的数据

```java
df.show();
```

打印DataFrame的元数据 (Schema)

```java
df.printSchema();

```

查询某列所有数据

```java
df.select("name").show();

```

查询某几列所有数据,并对列进行计算

```java
df.select(df.col("name"),df.col("age").plus(1)).show();

```

根据某一列的值进行过滤

```java
df.filter(df.col("age").gt(18)).show();

```

根据某一列进行分组,然后进行聚合

```java
df.groupBy(df.col("age")).count().show();
```

## RDD转DataFrame

### 为什么要将RDD转DataFrame?

> 因为这样的话,我们可以直接针对HDFS等任何可以构建为RDD的数据,使用Spark SQL进行SQL查询,这个功能很强大

### Spark SQL 支持2种方式来将RDD转为DataFrame

```markdown
1. 使用反射来推断包含了特定数据类型的RDD元数据.这种基于反射的方式,代码比较简洁,当你已经知道你的RDD的元素时,是一种不错的方式

2. 通过编程接口来创建DataFrame,你可以在程序运行时动态构建一份元数据,然后将其应用到已经存在的RDD上,代码比较冗长,
    但如果在编写程序时,还不知道RDD的元数据,只有在程序运行时,才能动态得知其元数据,只能通过动态构建元数据的方式
```

#### 使用反射的方式将RDD转换为Dataframe RDD2DataFrameReflection.java

```markdown
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
```

出现的问题:

> 1.Java Bean: Student.java 的序列化问题(不序列化会报错),public class Student implements Serializable

> 2.将RDD中的数据,进行映射,映射为student时,其RDD中student的属性顺序会乱(与文件中顺序不一致)


#### 以编程方式动态指定元数据,将RDD转换为DataFrame


```java
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
```


出现的问题:

>1.报错:不能直接从String转换为Integer的一个类型转换错误,说明有个数据,给定义成了String类型,结果使用的时候
要用Integer类型来使用,错误报在sql相关的代码中.在sql中,用到age<=18语法,所以强行将age转换为Integer来使用,
但之前有些步骤将age定义了String

---

### 通用的load和save操作  GenericLoadSave.java

```java
DataFrame usersDF = sqlContext.read().load("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/users.parquet");

usersDF.printSchema();

usersDF.show();

usersDF.select("name","favorite_color").write().save("/home/sotowang/Desktop/nameAndColors.parquet");
```

### 手动指定数据源类型  ManuallySpecifyOptions.java

默认为 parquet

```java
DataFrame usersDF = sqlContext.read()
                .format("parquet")
                .load("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/users.parquet");


usersDF.select("name","favorite_color")
        .write()
        .format("json")
        .save("/home/sotowang/Desktop/nameAndColors");
```

### saveMode  SaveModeTest.java

SaveMode.ErrorIfExists  文件存在会报错

```java
usersDF.save("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/users.parquet", SaveMode.ErrorIfExists);
```

SaveMode.Append   存在追加数据

```java
usersDF.save("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/users.parquet", SaveMode.Append);
```

SaveMode.Overwrite  覆盖


SaveMode.ignore 忽略


---

##  数据源Parquet之使用编程方式加载数据  ParquetLoadData.java

* Parquet是面向分析型业务的列式存储格式

列式存储与行式存储有哪些优势?

```markdown
1. 可以路过不符合条件的数据,只需要读取需要的数据,降低IO数据量
2. 压缩编码可以降低磁盘存储空间,由于同一列的数据类型是一样的,可以使用更高效的压缩编码(如Run Length Encoding和Delta Encoding) 进一步节约存储空间
3. 只读取需要的列,支持向量运算,能够获取更好的扫描性能

```

```java
DataFrame usersDF = sqlContext.read().parquet("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/users.parquet");

//将DataFrame注册为临时表,使用SQL查询需要的数据
usersDF.registerTempTable("users");

DataFrame userNameDf = sqlContext.sql("select name from users ");

//对查询出的DataFrame进行transformation操作,处理数据,然后打印
List<String> userNames = userNameDf.javaRDD().map(new Function<Row, String>() {
    public String call(Row row) throws Exception {
        return "Name: " + row.getString(0);
    }
}).collect();

for (String userName : userNames) {
    System.out.println(userName);
}
```

## 数据源Parquet之自动分区推断 

用户也许不希望Spark SQL自动推断分区列的数据类型。此时只要设置一个配置即可， 

```java
spark.sql.sources.partitionColumnTypeInference.enabled
```

，默认为true，即自动推断分区列的类型，设置为false，即不会自动推断类型。
禁止自动推断分区列的类型时，所有分区列的类型，就统一默认都是String。


## 数据源Parquet之合并元数据(默认关闭)  ParquetMergeSchema.java

案例:合并学生的基本信息和成绩信息的源数据

```java
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
```

结果

```markdown
+-----+----+-----+
| name| age|grade|
+-----+----+-----+
|  leo|  23| null|
| Jack|  25| null|
|marry|null|    A|
|  tom|null|    B|
+-----+----+-----+

```

## Json数据源  JSONDataSource.java
Spark SQL可以自动推断JSON文件的元数据,并且加载其数据,创建一个DataFrame,可以使用SQLContext.read.json()方法,针对一个元素类型为String
的RDD,或者是一个JSON文件

> 注意:这里使用的JSON文件与传统意义上的JSON文件是不一样的,每行都必须也只能包含一个单独的,包含的有效的JSON对象,不能让一个JSON对象分散在钓竿,否则会报错



案例: 查询成绩为80分以上的学生的基本信息与成绩信息

> 注:sqlContext.read().json(studentInfoJSONsRDD)  ==> 该API可以接受一个JavaRDD<String>直接转为DataFrame,与前面所讲的反射不一样

> 注: 默认DataFrame中将数字类型转为Long而不是Int,要将Long型转为Integer型需要Integer.valueOf(String.valueOf(row.getLong(1)))


## Hive数据源 (企业常用)   HiveDataSource.java



>操作Hive使用HiveContext而为是SQLContext.HiveContext继承自SQLContext,但是增加了在Hive元数据库中查找表,以及用HiveQL语法编写SQL功能,
除了sql()方法,HiveContext还提供了hql()方法,从而Hive语法来编译.
Hive中查询出来的数据是一个Row数组

将hive-site.xml拷贝到spark/conf目录下,将mysql connector 拷贝到spark/lib 目录下

* Spark SQL允许将数据保存到Hive表中,调用DataFrame的saveAsTable命令,即可将DaraFrame中的数据保存到Hive表中.与registerTempTable不同,
saveAsTable是会将DataFrame中的数据物化到Hive表中的,而且还会在Hive元数据库中创建表的元数据

* 默认情况下,saveAsTable会创建一张Hive Managed Table,也就是说,数据的位置都是由元数据中的信息控制的.当Managed Table被删除时,表中的数据也传动一并被
物理删除

* regiserTempTable只是注册一个临时的表,只要Spark Application重启或停止了,表就没了,而saveAsTable是物化的表,表会一直存在

* 调用HiveContext.table() 方法,还可以直接针对Hive中的表,创建一个DataFrame

案例: 查询分数大于80分的学生信息

*  创建HiveContext,注意:它接收的是sparkContext为参数而不是JavaSparkContext

>HiveContext hiveContext = new HiveContext(jsc.sc());

* 第一个功能:使用HiveContext的sql()/hql() 方法,可以执行Hive中能执行的HiveQL语句

```java
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

```

* 第二个功能,执行sql还可以返回DataFrame,用于查询

```java
      //第二个功能,执行sql还可以返回DataFrame,用于查询
      
              //执行sql查询,关联两张表,查询成绩大于80分的学生
              DataFrame goodStudentDF = hiveContext.sql(" SELECT si.name name, si.age age, ss.score score " +
                      " FROM student_infos si " +
                      " JOIN student_scores ss ON si.name =ss.name " +
                      " WHERE ss.score >= 80 ");

```

* 第三个功能,可以将DataFrame中的数据,理论上来说,DataFrame对应的RDD元素是Row即可将DataFrame中的数据保存到hive表中

```java
//第三个功能,可以将DataFrame中的数据,理论上来说,DataFrame对应的RDD元素是Row即可将DataFrame中的数据保存到hive表中

        //将DataFrame中的数据保存到good_student_infos
        hiveContext.sql("DROP TABLE IF EXISTS good_student_infos ");
        goodStudentDF.saveAsTable("good_student_infos");
```

* 第四个功能:可以用table() 方法针对hive表,直接创建,DataFrame

```java
       //第四个功能:可以用table() 方法针对hive表,直接创建DataFrame
               //然后针对good_student_infos表直接创建DataFrame
               Row[] goodStudentsRows = hiveContext.table("good_student_infos").collect();
       
               for (Row row : goodStudentsRows) {
                   System.out.println(row);
               }
```

注1：实际运行过程中出现报错如下:

>  MetaException(message:file:/user/hive/warehouse/src is not a directory or unable to create one)

解决方法:将hive-site.xml 放至resources目录下

注2:导入HDFS后,表内没数据,因为文件分隔符没有指定为空格

>hiveContext.sql("CREATE TABLE IF NOT EXISTS  student_infos(name STRING, age INT ) row format delimited fields terminated by ','");


## JDBC数据源 JDBCDataSource.java

案例:查询分数大于80分的学生信息

1. mysql创建数据库

> mysql> create database testdb;

> mysql> create table student_infos(name varchar(20),age integer);

> mysql> create table student_scores(name varchar(20),score integer);

> mysql> create table good_student_infos(name varchar(20),age integer,score integer);

> mysql> insert into student_infos values('leo',18),('marry',17),('jack',19);

> mysql> insert into student_scores values('leo',88),('marry',99),('jack',60);

2. 读取mysql表

```java
Map<String, String> options = new HashMap<String, String>();
options.put("url", "jdbc:mysql://sotowang-pc:3306/testdb");
options.put("user", "root");
options.put("password", "123456");

options.put("dbtable", "student_infos");
DataFrame studentInfoDF = sqlContext.read().format("jdbc").options(options).load();
```

3. 将DataFrame中的数据保存到Mysql数据表中

```java
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
                            "jdbc:mysql://sotowang-pc:3306/testdb", "root", "123456"
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
```

---

# Spark SQL高级内置函数

案例:根据每天的用户访问的购买日志统计每日的uv和销售额  (uv指:对用户进行去重以后的访问总数)


## 内置函数:countDistinct() DailyUV1.java

### 聚合函数用法 

>首先对DataFrame调用groupBy()方法,对某一列进行分组,
然后调用agg()方法,对数为内置函数,对见其源码

* 注:能过阅读Spark agg方法源码,得知需要手动导入api:__ 才能使用agg(countDistinct("userid"))方法

```java
import static org.apache.spark.sql.functions.countDistinct;
JavaRDD<Row> userAccessLogDistinctedRowRDD = userAccessLogRowDF.groupBy("date")
                .agg(countDistinct("userid"))  //(date,count(userid))
                .javaRDD();
```

## 内置函数:sum() DailySale.java

## 开窗函数以及top3销售额统计案例

### 最常用的函数: row_number() 实现分组取topN的逻辑  RowNumberWindowFunction.java

* 创建销售额表,sales表

```java
hiveContext.sql("drop table  if exists sales");
hiveContext.sql("create table if not exists sales ( " +
        " product STRING, " +
        " category STRING, " +
        "revenue BIGINT ) ");
hiveContext.sql("load data " +
        " local inpath '/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/sales.txt' " +
        " into table sales ");


```

*使用row_number()开窗函数row_number()
作用就是给你一份每个分组的数据 按照其排序打上一个分组内的行号
比如:有一个分组date=20181001,里面有3条数据,1122,1121,1124,
那么对这个分组的每一行使用row_num()开窗函数以后,三等依次会获得一个组内行号行号从1开始递增,
比如1122 1,1121 2,1124 3

```java
DataFrame top3SalesDF = hiveContext.sql("" +
        "select product,category,revenue " +
        " from ( " +
        " select " +
        " product," +
        "category," +
        "revenue," +
        //row_number() 语法说明:
        //首先,在select查询时,使用row_number()函数
        //其次,row_number()后面跟上over关键字
        //然后,括号中,是partition by ,根据哪个字段进行分组
        //其次,order by 起行组内排序
        //然后,row_number()就可以给组内行一个行号
        "row_number() over (partition by category order by revenue desc ) rank " +
        "from sales " +
        " ) tmp_sales " +
        "where rank <=3 ");

//将每组前三的数据保存到一个表中
hiveContext.sql("drop table if exists top3_sales ");
top3SalesDF.saveAsTable("top3_sales");
```



























