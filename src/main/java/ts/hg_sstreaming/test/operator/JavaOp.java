package ts.hg_sstreaming.test.operator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class JavaOp {

    private final String RUN_ENV =  "test";
    private  Dataset<Row> ds_right = null;
    private SparkSession session =  null;
    private String delimiter = ",";
    private Row[] rs_right =null;
    private String path =  null;
    public JavaOp(String path,SparkSession session){
        this.session = session;
        this.path = path;
    }
    public void init (){
        Dataset<Row> ds_right = session.read().option("header","true").option("delimiter",delimiter).csv(path);
        rs_right =(Row[]) ds_right.collect();
    }

    public String run(String inputPath,String outputPath){

        Dataset<Row> ds_left = session.read().option("header","true").option("delimiter",delimiter).csv(inputPath);

        Row[] rs_left = (Row[])ds_left.collect();


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

        ds_left.write().mode(SaveMode.Overwrite).option("header","true").csv(outputPath);
        System.out.println("in JAVA op: in"+inputPath+"  out:"+outputPath);
        return outputPath;
    }
}
