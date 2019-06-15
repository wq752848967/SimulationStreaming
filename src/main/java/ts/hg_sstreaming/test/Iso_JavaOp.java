package ts.hg_sstreaming.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Iso_JavaOp {
    String delimiter = "|";
    private static String masterUrl = "local[5]";
    //"/Users/wangqi/Desktop/FloK/sim/ios/left_data.csv";//
    private static String left_input = "hdfs://192.168.35.55:9000/flok/4665/DWFReadMySQL-1530083324_475428cc-5f2c-4291-915b-e5c3759b9405_0.output";
    //private static String right_input = "/Users/wangqi/Desktop/FloK/sim/ios/right_data.csv";
    private static String right_input = "hdfs://192.168.35.55:9000/flok/4665/csv_loader-1530083012_dafde4f6-9eda-404a-87df-c2fc51bf0dab_0.output";
    public static void main(String[] args) {

    }
    public void run(){
        SparkSession session = SparkSession.builder().master(masterUrl).getOrCreate();
        Dataset<Row> ds_left = session.read().option("header","true").option("delimiter",delimiter).csv(left_input);
        Dataset<Row> ds_right = session.read().option("header","true").option("delimiter",delimiter).csv(right_input);
        Row[] rs_left = (Row[])ds_left.collect();
        Row[] rs_right =(Row[]) ds_right.collect();
        for(Row r:rs_left){
            String deviceId = "1001"+r.getAs("device_id").toString();
            String period_end = r.getAs("period_end").toString();
            String period_start = r.getAs("period_start").toString();
            if(Integer.parseInt(deviceId)%5!=0){
                continue;
            }
            for(Row row:rs_right){
                String hotsId = row.getAs("machine_id").toString();
                String timestamp =row.getAs("timestamp").toString();

            }
        }
        ds_left.write().mode(SaveMode.Overwrite).option("header","true").csv("hdfs://192.168.35.55:9000/flok/4665/javaData.csv");
        session.close();
    }
}
