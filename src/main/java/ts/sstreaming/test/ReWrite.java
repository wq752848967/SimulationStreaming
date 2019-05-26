package ts.sstreaming.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReWrite {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("reW")
                .getOrCreate();
        Dataset<Row> ds  = spark.read().option("header","true").csv("hdfs://192.168.10.12:9000/flok/layer1_35all_J247.csv");
        //ds.write().text("hdfs://192.168.10.12:9000/flok/wordcount2");
        ds.show();
    }
}
