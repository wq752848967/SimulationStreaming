package ts.hg_sstreaming.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Iso_JavsSparkOp {
    String delimiter = "|";
    private  SparkSession session =  null;
    private static String masterUrl = "local[5]";
    //"/Users/wangqi/Desktop/FloK/sim/ios/left_data.csv";//
    private static String left_input = "hdfs://192.168.35.55:9000/flok/4665/DWFReadMySQL-1530083324_475428cc-5f2c-4291-915b-e5c3759b9405_0.output";
    //private static String right_input = "/Users/wangqi/Desktop/FloK/sim/ios/right_data.csv";
    private static String right_input = "hdfs://192.168.35.55:9000/flok/4665/csv_loader-1530083012_dafde4f6-9eda-404a-87df-c2fc51bf0dab_0.output";
    public static void main(String[] args) {

    }
    public void run(SparkSession spark){
        if(spark!=null){
            session = spark;
        }else{
            session = SparkSession.builder().master(masterUrl).getOrCreate();
        }
        //SparkSession


        //read data
        Dataset<Row> ds_left = session.read().option("header","true").option("delimiter",delimiter).csv(left_input);
        Dataset<Row> ds_right = session.read().option("header","true").option("delimiter",delimiter).csv(right_input);
        ds_left.registerTempTable("table_left");
        ds_right.registerTempTable("table_right");
//        ds_right.col("timestamp").cast("float");


        // add something cal op
        session.sql("select * from table_left where device_id!=128080").registerTempTable("filter_left");
        session.sql("select *,FROM_UNIXTIME(timestamp/1000+2678400,'yyyy-MM-dd HH:mm:ss.s') as new_timestamp from table_right").registerTempTable("filter_right");
        session.sql("select * from filter_left").show();
        session.sql("select * from filter_right,filter_left where  filter_left.period_start>=filter_right.new_timestamp and filter_left.period_end<=filter_right.new_timestamp").show();




        //result
        Dataset<Row> result_ds = session.sql("select * from table_left where device_id=128080");
        result_ds.write().mode(SaveMode.Overwrite).option("header","true").csv("hdfs://192.168.35.55:9000/flok/4665/sparkData.csv");
    }
}
