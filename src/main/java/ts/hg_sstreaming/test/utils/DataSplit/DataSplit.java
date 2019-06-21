package ts.hg_sstreaming.test.utils.DataSplit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSplit {
    public static void main(String[] args) {
        SparkSession session_java = SparkSession.builder().master("local[2]").getOrCreate();
        Dataset<Row> ds = session_java.read().option("header","true").csv("/Users/wangqi/Desktop/FloK/sim/ios/left_data_2.csv");
        ds.show();
        ds.write().option("header","true").option("delimiter","|").csv("hdfs://192.168.35.55:9000/flok/sim/sp2/left_2.csv");
    }
}
