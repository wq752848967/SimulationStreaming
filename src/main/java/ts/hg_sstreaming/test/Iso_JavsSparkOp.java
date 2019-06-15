package ts.hg_sstreaming.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Iso_JavsSparkOp {
    private  SparkSession session =  null;
    private static String masterUrl = "local[5]";
    private static String left_input = "/Users/wangqi/Desktop/FloK/sim/ios/left_data.csv";
    private static String right_input = "/Users/wangqi/Desktop/FloK/sim/ios/right_data.csv";
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
        Dataset<Row> ds_left = session.read().option("header","true").option("delimiter",",").csv(left_input);
        Dataset<Row> ds_right = session.read().option("header","true").option("delimiter",",").csv(right_input);
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
