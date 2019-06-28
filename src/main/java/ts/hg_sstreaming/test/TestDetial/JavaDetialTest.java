package ts.hg_sstreaming.test.TestDetial;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import ts.hg_sstreaming.test.utils.TimeUtils;
import java.util.ArrayList;
import java.util.List;

public class JavaDetialTest {
    private static SparkSession session = null;
    private static Row[] rs_right = null;
    private static String RUN_ENV = "product";
    private static String delimiter = "|";
    private static String path = "|";
    private static int index = 0;
    private static ArrayList<String> logs = new ArrayList<>();
    public static void main(String[] args) {

        long start = System.currentTimeMillis();
        session = SparkSession.builder().master("local[2]").getOrCreate();
        path = "hdfs://192.168.35.55:9000/flok/4665/csv_loader-1530083012_dafde4f6-9eda-404a-87df-c2fc51bf0dab_0.output";

        init();
        List<String> path_arr  = new ArrayList<String>();
        if(args[0].equals("one")){
            path_arr.add("hdfs://192.168.35.55:9000/flok/4665/DWFReadMySQL-1530083324_475428cc-5f2c-4291-915b-e5c3759b9405_0.output");
        }
        else{
            path_arr.add("hdfs://192.168.35.55:9000/flok/sim/sp4/1.csv");
            path_arr.add("hdfs://192.168.35.55:9000/flok/sim/sp4/2.csv");
            path_arr.add("hdfs://192.168.35.55:9000/flok/sim/sp4/3.csv");
            path_arr.add("hdfs://192.168.35.55:9000/flok/sim/sp4/4.csv");
        }


        for(String p:path_arr){
            long s = System.currentTimeMillis();
            run(p,"hdfs://192.168.35.55:9000/flok/sim/detial/data"+index+".csv");
            long e = System.currentTimeMillis();
            logs.add("file "+index+" start:"+ TimeUtils.tranTime(s)+"  end:"+TimeUtils.tranTime(e)+"  cost:"+(e-s)/1000);
            index++;
        }
        long end = System.currentTimeMillis();
        System.out.println(index+" total:"+(end-start)/1000);
        for (String s:logs){
            System.out.println(s);
        }

    }
    public static void init (){
        Dataset<Row> ds_right = session.read().option("header","true").option("delimiter","|").csv(path);
        rs_right =(Row[]) ds_right.collect();
        //ds_right.show();

    }

    public static String run(String inputPath,String outputPath){
        long read_s = System.currentTimeMillis();
        Dataset<Row> ds_left = session.read().option("header","true").option("delimiter",delimiter).csv(inputPath);
        //ds_left.show();
        Row[] rs_left = (Row[])ds_left.collect();
        long read_e = System.currentTimeMillis();



        if(RUN_ENV.equals("product")){
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
        }
        long cal_e = System.currentTimeMillis();
        ds_left.write().mode(SaveMode.Overwrite).option("header","true").csv(outputPath);
        System.out.println("in JAVA op: "+inputPath+"  out:"+outputPath);
        long write_e = System.currentTimeMillis();
        logs.add(index+" read cost:"+((read_e-read_s)/100)+" start:"+TimeUtils.tranTime(read_s)+"   end:"+TimeUtils.tranTime(read_e));
        logs.add(index+" cal cost:"+((cal_e-read_e)/1000)+" start:"+TimeUtils.tranTime(read_e)+"   end:"+TimeUtils.tranTime(cal_e));
        logs.add(index+" write cost:"+((write_e-cal_e)/100)+" start:"+TimeUtils.tranTime(cal_e)+"   end:"+TimeUtils.tranTime(write_e));

        return outputPath;
    }
}
