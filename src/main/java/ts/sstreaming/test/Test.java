package ts.sstreaming.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Test {
    public static void main(String[] args) {
        String masterUrl = "spark://192.168.10.12:7077";
        SparkSession session = SparkSession.builder().master(masterUrl).getOrCreate();
        Dataset<Row> ds = session.read().option("header","true").csv("hdfs://192.168.10.12:9000/flok/sim_data_id.csv");
        Dataset<Row> new_ds = ds.limit(3661658);
        new_ds.write().option("header","true").mode(SaveMode.Overwrite).csv("hdfs://192.168.10.12:9000/flok/sim_data_small.csv");
    }
}
