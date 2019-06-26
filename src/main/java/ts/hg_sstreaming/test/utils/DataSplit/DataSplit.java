package ts.hg_sstreaming.test.utils.DataSplit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DataSplit {
    public static void main(String[] args) {
        SparkSession session_java = SparkSession.builder().master("local[2]").getOrCreate();
        Dataset<Row> ds = session_java.read().option("header","true").option("delimiter","|").csv("/Users/wangqi/Desktop/FloK/sim/left3/1.csv");
        //hdfs://192.168.35.55:9000/flok/4665/DWFReadMySQL-1530083324_475428cc-5f2c-4291-915b-e5c3759b9405_0.output
        ds.show();
        //ds = ds.repartition(3);
        //Dataset<Row> ds_right =session_java.read().option("header","true").option("delimiter","|").csv("hdfs://192.168.35.55:9000/flok/4665/csv_loader-1530083012_dafde4f6-9eda-404a-87df-c2fc51bf0dab_0.output");
//        ds.show();
        //System.out.println(ds.count());
//        ds.registerTempTable("table_left");
//        ds_right.registerTempTable("table_right");
//        session_java.sql("select * from table_left where device_id!=128080").registerTempTable("filter_left");
//        session_java.sql("select *,FROM_UNIXTIME(timestamp/1000+2678400,'yyyy-MM-dd HH:mm:ss.s') as new_timestamp from table_right").registerTempTable("filter_right");
//        session_java.sql("select * from filter_left").show();
//        session_java.sql("select * from filter_right,filter_left where  filter_left.period_start>=filter_right.new_timestamp and filter_left.period_end<=filter_right.new_timestamp").show();
          //ds.write().option("header","true").option("delimiter","|").mode(SaveMode.Overwrite).csv("hdfs://192.168.35.55:9000/flok/sim/sp3/left_3.csv");

    }
}
