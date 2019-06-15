package ts.hg_sstreaming.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Iso_JavaOp {
    private static String masterUrl = "local[5]";
    private static String left_input = "/Users/wangqi/Desktop/FloK/sim/ios/left_data.csv";
    private static String right_input = "/Users/wangqi/Desktop/FloK/sim/ios/right_data.csv";
    public static void main(String[] args) {

    }
    public void run(){
        SparkSession session = SparkSession.builder().master(masterUrl).getOrCreate();
        Dataset<Row> ds_left = session.read().option("header","true").option("delimiter",",").csv(left_input);
        Dataset<Row> ds_right = session.read().option("header","true").option("delimiter",",").csv(right_input);
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
