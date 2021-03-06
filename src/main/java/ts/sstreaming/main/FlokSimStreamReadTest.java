package ts.sstreaming.main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ts.sstreaming.engine.SparkStreamSchedular;

public class FlokSimStreamReadTest {
    private static String masterUrl = "local[2]";
    //private static String definition = "[{'nodeid_in_workflow':'d01a22b1-e429-458a-b1d1-c7046d18a792','component_id':'34b8de51c05141c8af20210f4c7eec71','params':{'path':'hdfs://192.168.10.12:9000/flok/layer1_35all_J247.csv','delimiter':',','header_text':'','returns':[]},'requirements_id':[],'ports':[],'outputs':[],'algorithm':'HDFSReader'},{'nodeid_in_workflow':'ecd44ba3-2593-429c-b6a6-113090bce31e','component_id':'01ae2cb1ac32437d823a34ff55373876','params':{'attr_names':'host,align_time','returns':[]},'requirements_id':[['d01a22b1-e429-458a-b1d1-c7046d18a792']],'ports':[[0]],'outputs':[],'algorithm':'SelectAttribute'},{'nodeid_in_workflow':'1fa29777-cc43-40da-b8ba-92d9dad54a3f','component_id':'0ba7d71b30e24759b6200b3a615f57c6','params':{'attr_names':'host'},'requirements_id':[['ecd44ba3-2593-429c-b6a6-113090bce31e']],'ports':[[0]],'outputs':['test://123456789/wq/flok/test_result.csv'],'algorithm':'SelectAttribute'}]";
    private static String definition  = "[{'nodeid_in_workflow':'49afe634-8778-405b-9737-ae95f28e833c','component_id':'34b8de51c05141c8af20210f4c7eec71','params':{'path':'hdfs://192.168.10.12:9000/flok/layer1_35all_J247.csv','delimiter':',','header_text':'','returns':[]},'requirements_id':[],'ports':[],'outputs':[],'algorithm':'HDFSReader'},{'nodeid_in_workflow':'6710ca05-156b-4a40-9bf8-197b94787bc6','component_id':'2946c261700747e18c709ee955a74857','params':{'sql_expr':'select * from t where align_time >= \\'2019-01-23 00:00:00+0800\\' and align_time <= \\'2019-01-23 18:00:00+0800\\'','table_name':'t','returns':[]},'requirements_id':[['49afe634-8778-405b-9737-ae95f28e833c']],'ports':[[0]],'outputs':[],'algorithm':'SqlExprExecute'},{'nodeid_in_workflow':'52b8c98c-00ce-4ade-96f8-226588de5726','component_id':'93c8d637bd1a4933a7d4898609a5e0a1','params':{'conditions':'J_0001_00_247<7000','returns':[]},'requirements_id':[['6710ca05-156b-4a40-9bf8-197b94787bc6']],'ports':[[0]],'outputs':[],'algorithm':'Filter'},{'nodeid_in_workflow':'db7c4f6b-83e4-441e-a904-1a4001f84012','component_id':'01ae2cb1ac32437d823a34ff55373876','params':{'attr_names':'host,align_time,J_0001_00_247'},'requirements_id':[['52b8c98c-00ce-4ade-96f8-226588de5726']],'ports':[[0]],'outputs':['hdfs://flok/test_result.csv'],'algorithm':'SelectAttribute'}]";
    //private static String jarPath = "file:///Users/wangqi/Downloads/workflow-0.1.0-SNAPSHOT-jar-with-dependencies.jar";
    private static String jarPath_server  = "file:///tmp/spark-warehouse/user_defined/algorithm/workflow-0.1.0-SNAPSHOT-jar-with-dependencies.jar";
    private static int threadNum = 5;
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().master(masterUrl).getOrCreate();
        Dataset<Row> ds = session.read().option("header","true").csv("hdfs://192.168.10.12:9000/flok/sim_data_small.csv");
        ds.registerTempTable("t");
        Dataset<Row> new_ds = session.sql("select max(id),distinct(host) from t group by id");

        new_ds.show();
    }
}
