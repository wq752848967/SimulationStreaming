package ts.sstreaming.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import ts.workflow.lib.FloKAlgorithm;
import ts.workflow.lib.FloKDataSet;
import ts.workflow.operator.SqlExprExecute;

import java.util.HashMap;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class SparkStructToBatch {
    //private static String jarPath = "file:///Users/wangqi/Downloads/workflow-0.1.0-SNAPSHOT-jar-with-dependencies.jar";
    public static void main(String[] args)throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        StructType scheme = new StructType()
                .add("id",IntegerType)
                .add("align_time",StringType)
                .add("host",StringType)
                .add("J_0001_00_247",StringType);
        DataSourceOp dataSourceOp = new DataSourceOp(spark);
        //ObjectLoaderInter loader = new JarObjectLoaderImpl();
        long start_time = System.currentTimeMillis();
        Dataset<Row> ds  = dataSourceOp.getStreamDsRow("/Users/wangqi/Desktop/FloK/sim/sim_test_small.csv",scheme);
//        FloKAlgorithm flokNode = new SqlExprExecute();
//        flokNode.sparkSession = spark;
//        FloKDataSet flok_ds = new FloKDataSet();
//        flok_ds.addDF(ds);
//        HashMap<String,String> param = new HashMap<>();
//        param.put("sql_expr","select max(id) as id, max(host) as host,max(J_0001_00_247) as J_0001_00_247 from t group by id");
//        param.put("table_name","t");
//        FloKDataSet flok_result_ds = flokNode.run(flok_ds,param);
        ds.registerTempTable("t");
        Dataset<Row> sql_dd = spark.sql("select max(id) as id, max(host) as host,max(J_0001_00_247) as J_0001_00_247 from t group by id");

//        FloKAlgorithm flokNode2 = new SqlExprExecute();
//        flokNode2.sparkSession = spark;
//
//        HashMap<String,String> param2 = new HashMap<>();
//        param2.put("sql_expr","select max(unq_id) as unq_id, max(host) as host,max(J_0001_00_247) as J_0001_00_247 from t2 group by J_0001_00_247");
//        param2.put("table_name","t2");
//        FloKDataSet flok_result_ds2 = flokNode2.run(flok_result_ds,param2);
        StreamingQuery query = sql_dd.writeStream()
                .queryName("result")
                .outputMode("complete")
                .format("memory")
                .start();
        long end_1 = System.currentTimeMillis();
        System.out.println("总时间1：:"+((end_1-start_time)/1000)+"");


//        long end_2 = System.currentTimeMillis();
//        System.out.println("总时间2：:"+((end_2-start_time)/1000)+"");



        spark.sql("select * from result").show();


        query.awaitTermination();
    }
}
