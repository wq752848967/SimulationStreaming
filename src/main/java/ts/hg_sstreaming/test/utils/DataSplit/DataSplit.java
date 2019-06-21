package ts.hg_sstreaming.test.utils.DataSplit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DataSplit {
    public static void main(String[] args) {
        SparkSession session_java = SparkSession.builder().master("local[2]").getOrCreate();
        Dataset<Row> ds = session_java.read().option("header","true").option("delimiter","|").csv("hdfs://192.168.35.55:9000/flok/sim/sp2/left_1.csv");
        //double[] s = {0.5,0.5};
        //Dataset<Row>[] dss =  ds.randomSplit(s);
        Dataset<Row> ds_right =session_java.read().option("header","true").option("delimiter","|").csv("hdfs://192.168.35.55:9000/flok/4665/csv_loader-1530083012_dafde4f6-9eda-404a-87df-c2fc51bf0dab_0.output");
//        ds.show();
        //System.out.println(ds.count());
        ds.registerTempTable("table_left");
        ds_right.registerTempTable("table_right");
        session_java.sql("select * from table_left where device_id!=128080").registerTempTable("filter_left");
        session_java.sql("select *,FROM_UNIXTIME(timestamp/1000+2678400,'yyyy-MM-dd HH:mm:ss.s') as new_timestamp from table_right").registerTempTable("filter_right");
        session_java.sql("select * from filter_left").show();
        session_java.sql("select * from filter_right,filter_left where  filter_left.period_start>=filter_right.new_timestamp and filter_left.period_end<=filter_right.new_timestamp").show();
//          dss[0].write().option("header","true").option("delimiter","|").mode(SaveMode.Overwrite).csv("hdfs://192.168.35.55:9000/flok/sim/sp2/left_1.csv");
//        dss[0].write().option("header","true").option("delimiter","|").mode(SaveMode.Overwrite).csv("hdfs://192.168.35.55:9000/flok/sim/sp2/left_2.csv");
    }
}
