package ts.hg_sstreaming.test.TestDetial;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CalTest {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();
        String path = "hdfs://192.168.35.55:9000/flok/4665/csv_loader-1530083012_dafde4f6-9eda-404a-87df-c2fc51bf0dab_0.output";
        Dataset<Row> ds_right = session.read().option("header","true").option("delimiter","|").csv(path);
        Row[] rs_right =(Row[]) ds_right.collect();
        String totalPath = "hdfs://192.168.35.55:9000/flok/4665/DWFReadMySQL-1530083324_475428cc-5f2c-4291-915b-e5c3759b9405_0.output";
        if(args[0].equals("one")){
            Dataset<Row> ds_left = session.read().option("header","true").option("delimiter","|").csv(totalPath);
            Row[] rs_left = (Row[])ds_left.collect();
            long start = System.currentTimeMillis();
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
            long end = System.currentTimeMillis();
            System.out.println("total:"+(end-start)/1000);


        }
        else{
            int index = 0;
            long total = 0;

            for (index= 1; index <7; index++) {
                Dataset<Row> ds_left = session.read().option("header","true").option("delimiter","|").csv("hdfs://192.168.35.55:9000/flok/sim/sp6/"+index+".csv");
                Row[] rs_left = (Row[])ds_left.collect();
                long start = System.currentTimeMillis();
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
                long end = System.currentTimeMillis();
                total+=end-start;
            }
            System.out.println("total s:"+total/1000);

        }
    }
}
