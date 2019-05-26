package ts.sstreaming.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import ts.sstreaming.utils.impl.JarObjectLoaderImpl;
import ts.sstreaming.utils.inter.ObjectLoaderInter;
import ts.workflow.lib.FloKAlgorithm;
import ts.workflow.lib.FloKDataSet;
import ts.workflow.operator.RDDOper;
import ts.workflow.operator.SqlExprExecute;

import java.util.HashMap;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class SparkStructTest {
    private static String jarPath = "file:///Users/wangqi/Downloads/workflow-0.1.0-SNAPSHOT-jar-with-dependencies.jar";
    public static void main(String[] args)throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://192.168.10.12:7077")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        StructType scheme = new StructType().add("id",IntegerType)
                .add("align_time",StringType)
                .add("host",StringType)
                .add("J_0001_00_247",StringType);
        DataSourceOp dataSourceOp = new DataSourceOp(spark);
        //ObjectLoaderInter loader = new JarObjectLoaderImpl();
        long start_time = System.currentTimeMillis();
        Dataset<Row> ds  = dataSourceOp.getStreamDsRow("hdfs://192.168.10.12:9000/flok/sim_data_small.csv",scheme);
        FloKAlgorithm flokNode = new SqlExprExecute();
        flokNode.sparkSession = spark;
        FloKDataSet flok_ds = new FloKDataSet();
        flok_ds.addDF(ds);
        HashMap<String,String> param = new HashMap<>();
        param.put("sql_expr","select max(id) as id, max(host) as host,max(J_0001_00_247) as J_0001_00_247 from t group by id");
        param.put("table_name","t");
        FloKDataSet flok_result_ds = flokNode.run(flok_ds,param);



        FloKAlgorithm flokNode2 = new SqlExprExecute();
        flokNode.sparkSession = spark;

        HashMap<String,String> param2 = new HashMap<>();
        param2.put("sql_expr","select t_1.id as id,t_1.host as host ,t_1.J_0001_00_247 as J_0001_00_247,t_2.id  as id2 from t2 as t_1 INNER JOIN t2 as t_2 on t_1.id > (t_2.id-10) and  t_1.id < (t_2.id+10)");
        param2.put("table_name","t2");
        FloKDataSet flok_result_ds2 = flokNode2.run(flok_result_ds,param2);
        StreamingQuery query = flok_result_ds2.get(0).writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        long end_1 = System.currentTimeMillis();
        System.out.println("总时间1：:"+((end_1-start_time)/1000)+"");
        query.awaitTermination();
        long end_2 = System.currentTimeMillis();
        System.out.println("总时间2：:"+((end_2-start_time)/1000)+"");
    }
}
