package ts.sstreaming.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class ReWrite {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://192.168.10.12:7077")
                .appName("reW")
                .getOrCreate();
        Dataset<String> ds  = spark.read().textFile("hdfs://192.168.10.12:9000/flok/wordcount.txt");
        ds.write().text("hdfs://192.168.10.12:9000/flok/wordcount2");
    }
}
