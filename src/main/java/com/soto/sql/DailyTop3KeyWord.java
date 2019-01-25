package com.soto.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 每日top3 热点搜索词统计
 */
public class DailyTop3KeyWord {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("DailyTop3KeyWord")
                .setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        HiveContext hiveContext = new HiveContext(jsc.sc());

        //伪造一份数据,查询条件
        //注: 实际企业项目中,很可能通过J2EE平台插入到Mysql表中,然后用Spring构架和ORM框架去提取MySQL表中的查询条件
        Map<String, List<String>> queryParaMap = new HashMap<String, List<String>>();
        queryParaMap.put("city", Arrays.asList("beijing"));
        queryParaMap.put("platform", Arrays.asList("android"));
        queryParaMap.put("version", Arrays.asList("1.0","1.2","1.5","2"));


        JavaRDD<String> rawRDD = jsc.textFile("hdfs://sotowang-pc:9000/spark_study/keyword.txt");
//        JavaRDD<String> rawRDD = jsc.textFile("/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/SparkSQLProject/src/resources/keyword.txt");

        final Broadcast<Map<String, List<String>>> queryParamMapBroadCast =
                jsc.broadcast(queryParaMap);

        JavaRDD<String> filteredRDD = rawRDD.filter(new Function<String, Boolean>() {
            public Boolean call(String v1) throws Exception {

                //获取城市,平台,版本
                String[] logSplited = v1.split(" ");
                String city = logSplited[3];
                String platform = logSplited[4];
                String version = logSplited[5];

                //与查询条件对比
                Map<String, List<String>> queryParamMap = queryParamMapBroadCast.value();


                List<String> cities = queryParamMap.get("city");
                if (cities.size()>0 && !cities.contains(city)) {
                    return false;
                }
                List<String> platforms = queryParamMap.get("platform");
                if (platforms.size() > 0 && !platforms.contains(platform)) {
                    return false;
                }
                List<String> versions = queryParamMap.get("version");
                if (versions.size()>0 && !versions.contains(version)) {
                    return false;
                }

                return true;
            }
        });

        // 将数据转换为"(日期_搜索词, 用户)" 的格式
        JavaPairRDD<String, String> dateKeywordUserRDD = filteredRDD.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String log) throws Exception {
                //获取date user keyword
                String[] logSplited = log.split(" ");
                String date = logSplited[0];
                String user = logSplited[1];
                String keyword = logSplited[2];
                return new Tuple2<String, String>(date + "_" + keyword, user);
            }
        });

        //对每天每个搜索词的用户进行去重操作,并统计去重后的数量,即为每天每个搜索词的uv,最后获得"(日期_搜索词,uv)"
        JavaPairRDD<String,Iterable<String>> dateKeywordUsersRDD = dateKeywordUserRDD.groupByKey();
        JavaPairRDD<String, Long> dateKeywordUVRDD = dateKeywordUsersRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> dateKeywordUsers) throws Exception {
                String dateKeyword = dateKeywordUsers._1;
                Iterator<String> users = dateKeywordUsers._2.iterator();

                //对用户进行去重,并统计去重后的数量
                List<String> distinctUsers = new ArrayList<String>();
                while (users.hasNext()) {
                    String user = users.next();
                    if (!distinctUsers.contains(user)) {
                        distinctUsers.add(user);
                    }
                }
                long uv = distinctUsers.size();
                return new Tuple2<String, Long>(dateKeyword, uv);
            }
        });

        //将得到的每天每个搜索词的uv,RDD,映射为元素类型为Row的RDD,将该RDD转换为DataFrame
        JavaRDD<Row> dateKeywordUvRDD = dateKeywordUVRDD.map(new Function<Tuple2<String, Long>, Row>() {
            public Row call(Tuple2<String, Long> dateKeywordUv) throws Exception {
                String date = dateKeywordUv._1.split("_")[0];
                String keyword = dateKeywordUv._1.split("_")[1];
                long uv = dateKeywordUv._2;
                return RowFactory.create(date, keyword, uv);
            }
        });

        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyword", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true)

        );

        StructType structType = DataTypes.createStructType(structFields);
        DataFrame dateKeywordUvDF = hiveContext.createDataFrame(dateKeywordUvRDD, structType);

        //将DataFrame注册为临时表,使用Spark SQL的开窗函数,来统计每天的uv数量排名前3的搜索词,以及它的搜索uv,最后获取,是一个DataFrame
        dateKeywordUvDF.registerTempTable("daily_keyword_uv");

        DataFrame dailyTop3KeywordDF = hiveContext.sql(
                " select date,keyword,uv " +
                        " from ( " +
                        "select date," +
                        "keyword," +
                        "uv, " +
                        " row_number()  over (partition by date order by uv desc) rank " +
                        " from daily_keyword_uv " +
                        " ) tmp " +
                        " where rank <=3 "
        );

        //将DataFrame转换为RDD,继续操作按照每天日期来进行分级,并进行映射,计算出每天的top3搜索词的搜索uv的总数
        JavaRDD<Row> dailyTop3KeywordRDD = dailyTop3KeywordDF.javaRDD();
        JavaPairRDD<String, String> top3DateKeywordUvRDD = dailyTop3KeywordRDD.mapToPair(new PairFunction<Row, String, String>() {
            public Tuple2<String, String> call(Row row) throws Exception {
                String date = String.valueOf(row.get(0));
                String keyword = String.valueOf(row.get(1));
                long uv = Long.valueOf(String.valueOf(row.get(2)));
                return new Tuple2<String, String>(date, keyword + "_" + uv);
            }
        });
        JavaPairRDD<String, Iterable<String>> top3DateKeywordsRDD = top3DateKeywordUvRDD.groupByKey();

        JavaPairRDD<Long, String> uvDateKeywordsRDD = top3DateKeywordsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
            public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                String date = tuple2._1;
                String dateKeywords = date ;
                Long totalUv = 0l;
                Iterator<String> keywordUvIterator = tuple2._2.iterator();
                while (keywordUvIterator.hasNext()) {
                    String keywordUv = keywordUvIterator.next();
                    Long uv = Long.valueOf(keywordUv.split("_")[1]);
                    totalUv += uv;
                    dateKeywords += "," + keywordUv;
                }

                return new Tuple2<Long, String>(totalUv, dateKeywords);
            }
        });


        //按照每天的总搜索Uv进行倒序排序
        JavaPairRDD<Long, String> sortedUvDateKeywordsRDD = uvDateKeywordsRDD.sortByKey(false);

        System.out.println(sortedUvDateKeywordsRDD.take(10));


        //再次映射,将排序后的数据映射回原始形式,Iterable<Row>
        JavaRDD<Row> sortedRowRDD = sortedUvDateKeywordsRDD.flatMap(new FlatMapFunction<Tuple2<Long, String>, Row>() {
            public Iterable<Row> call(Tuple2<Long, String> tuple2) throws Exception {
                String dateKeywords = tuple2._2;
                String[] dateKeywordsSplited = dateKeywords.split(",");
                String date = dateKeywordsSplited[0];
                List<Row> rows = new ArrayList<Row>();
                rows.add(RowFactory.create(date, dateKeywordsSplited[1].split("_")[0],
                        Long.valueOf(dateKeywordsSplited[1].split("_")[1])));
                rows.add(RowFactory.create(date, dateKeywordsSplited[1].split("_")[0],
                        Long.valueOf(dateKeywordsSplited[2].split("_")[1])));
                rows.add(RowFactory.create(date, dateKeywordsSplited[1].split("_")[0],
                        Long.valueOf(dateKeywordsSplited[3].split("_")[1])));

                return rows;
            }
        });

        //将最终的数据转换为DataFrame,并保存到Hive表中
        DataFrame finalDF = hiveContext.createDataFrame(sortedRowRDD, structType);

        hiveContext.sql("drop table if exists daily_top3_keyword_uv");
        finalDF.saveAsTable("daily_top3_keyword_uv");


        jsc.close();
    }







}
