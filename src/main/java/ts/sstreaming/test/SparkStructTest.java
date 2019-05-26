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
                .master("local[2]")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        StructType scheme = new StructType().add("id",IntegerType)
                .add("align_time",StringType)
                .add("host",StringType)
                .add("J_0001_00_247",StringType);
        DataSourceOp dataSourceOp = new DataSourceOp(spark);
        //ObjectLoaderInter loader = new JarObjectLoaderImpl();

        Dataset<Row> ds  = dataSourceOp.getStreamDsRow("/Users/wangqi/Desktop/FloK/sim/sim_test_small.csv",scheme);
        FloKAlgorithm flokNode = new SqlExprExecute();
        flokNode.sparkSession = spark;
        FloKDataSet flok_ds = new FloKDataSet();
        flok_ds.addDF(ds);
        HashMap<String,String> param = new HashMap<>();
        param.put("sql_expr","select max(id) as id, max(host) as host,max(J_0001_00_247) as J_0001_00_247 from t group by id");
        param.put("table_name","t");
        FloKDataSet flok_result_ds = flokNode.run(flok_ds,param);
        StreamingQuery query = flok_result_ds.get(0).writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }
}
