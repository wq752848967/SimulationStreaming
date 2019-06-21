package ts.hg_sstreaming.test.operator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkOp {

    private final String RUN_ENV =  "test";
    private Dataset<Row> ds_right = null;
    private SparkSession session =  null;
    private String delimiter = ",";
    private String path = null;
    public SparkOp(String path,SparkSession session){
        this.session = session;
        this.path = path;

    }
    public void init(){
        ds_right = session.read().option("header","true").option("delimiter","|").csv(path);
    }
    public String run(String inputPath,String outputPath){



        //read data
        System.out.println("before spark :"+inputPath+" "+outputPath);
        Dataset<Row> ds_left = session.read().option("header","true").option("delimiter",delimiter).csv(inputPath);
        //ds_left.show();
        ds_left.registerTempTable("table_left");
        ds_right.registerTempTable("table_right");
//        ds_right.col("timestamp").cast("float");


        // add something cal op
        if(RUN_ENV.equals("product")){
            session.sql("select * from table_left where device_id!=128080").registerTempTable("filter_left");
            session.sql("select *,FROM_UNIXTIME(timestamp/1000+2678400,'yyyy-MM-dd HH:mm:ss.s') as new_timestamp from table_right").registerTempTable("filter_right");
            session.sql("select * from filter_left").show();
            session.sql("select * from filter_right,filter_left where  filter_left.period_start>=filter_right.new_timestamp and filter_left.period_end<=filter_right.new_timestamp").show();
        }




        //result
        Dataset<Row> result_ds = session.sql("select * from table_left where device_id=128080");
        System.out.println("before write");
        result_ds.write().mode(SaveMode.Overwrite).option("header","true").csv(outputPath);
        System.out.println("in Spark op: in"+inputPath+"  out:"+outputPath);
        return outputPath;
    }
}
