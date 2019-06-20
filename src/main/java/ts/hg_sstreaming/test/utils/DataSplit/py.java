package ts.hg_sstreaming.test.utils.DataSplit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class py {
    public static void main(String[] args) {
        SparkSession session_java = SparkSession.builder().master("local[2]").getOrCreate();
        //Dataset<Row> ds_left = session_java.read().option("header","true").option("delimiter",delimiter).csv(inputPath);
        Dataset<Row> ds = session_java.read().option("header","true").option("delimiter",",").csv("hdfs://192.168.35.55:9000/flok/sim/java_out/0.csv");
        ds.show();
//        ds = ds.coalesce(1);
//        ds.write().option("header","true").csv("/Users/wangqi/Desktop/FloK/sim/ios/left_data_one.csv");
    }
}
