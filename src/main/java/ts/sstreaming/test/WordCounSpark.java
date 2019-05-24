package ts.sstreaming.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class WordCounSpark implements Serializable {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();

        DataSourceOp dsource = new DataSourceOp(spark);
        boolean batch = false;
        Dataset<String> ds_input = null;
        if(batch){
            ds_input = dsource.getBatchDs("/Users/wangqi/Desktop/FloK/sim/sim_text.csv");

        }else{
            ds_input = dsource.getStreamDs("/Users/wangqi/Desktop/FloK/sim/sim_text.csv");
        }


        Dataset<String> ds_sum = ds_input.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> result = ds_sum.groupBy("value").count();

        if(batch){
            result.show();
        }
        else{
            StreamingQuery query = result.writeStream()
                    .outputMode("complete")
                    .format("console")
                    .start();

            query.awaitTermination();
        }

//        String masterUrl = "spark://192.168.10.12:7077";
//        AtomicInteger val  = new AtomicInteger();
//        SparkConf sparkConf = new SparkConf()
//                .setAppName("wordCountLocal")
//                .setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//
//
//        // 3.读取本地文件
//        JavaRDD<String> lines = sc.textFile("hdfs://192.168.10.12:9000/flok/wordcount2");
//
//        // 4.每行以空格切割
//        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            public Iterator<String> call(String t) throws Exception {
//                return Arrays.asList(t.split(" ")).iterator();
//            }
//        });
//
//        // 5.转换为 <word,1>格式
//        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
//            public Tuple2<String, Integer> call(String t) throws Exception {
//                return new Tuple2<String, Integer>(t, 1);
//            }
//        });
//
//        // 6.统计相同Word的出现频率
//        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        });
//
//        // 7.执行action，将结果打印出来
//        wordCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
//            public void call(Tuple2<String, Integer> t) throws Exception {
//                System.out.println(t._1()+" "+t._2());
//            }
//        });


    }
}
