package ts.sstreaming.main;

import org.apache.spark.sql.SparkSession;
import ts.sstreaming.engine.SparkStreamSchedular;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlokSimStreamTest {
    public static List<String> timeLog = new ArrayList<>();
    private static String masterUrl = "spark://192.168.10.12:7077";
    //private static String definition = "[{'nodeid_in_workflow':'d01a22b1-e429-458a-b1d1-c7046d18a792','component_id':'34b8de51c05141c8af20210f4c7eec71','params':{'path':'hdfs://192.168.10.12:9000/flok/layer1_35all_J247.csv','delimiter':',','header_text':'','returns':[]},'requirements_id':[],'ports':[],'outputs':[],'algorithm':'HDFSReader'},{'nodeid_in_workflow':'ecd44ba3-2593-429c-b6a6-113090bce31e','component_id':'01ae2cb1ac32437d823a34ff55373876','params':{'attr_names':'host,align_time','returns':[]},'requirements_id':[['d01a22b1-e429-458a-b1d1-c7046d18a792']],'ports':[[0]],'outputs':[],'algorithm':'SelectAttribute'},{'nodeid_in_workflow':'1fa29777-cc43-40da-b8ba-92d9dad54a3f','component_id':'0ba7d71b30e24759b6200b3a615f57c6','params':{'attr_names':'host'},'requirements_id':[['ecd44ba3-2593-429c-b6a6-113090bce31e']],'ports':[[0]],'outputs':['test://123456789/wq/flok/test_result.csv'],'algorithm':'SelectAttribute'}]";
    private static String definition  = "[{'nodeid_in_workflow':'49afe634-8778-405b-9737-ae95f28e833c','component_id':'34b8de51c05141c8af20210f4c7eec71','params':{'path':'hdfs://192.168.10.12:9000/flok/sim_data_small.csv','delimiter':',','header_text':'','returns':[]},'requirements_id':[],'ports':[],'algorithm':'HDFSReader','outputs':[]},{'nodeid_in_workflow':'256363a0-4dee-4436-96ad-405dde910d8e','component_id':'2946c261700747e18c709ee955a74857','params':{'table_name':'t','sql_expr':'select max(id) as id, max(host) as host,max(J_0001_00_247) as J_0001_00_247 from t group by id','returns':[]},'requirements_id':[['49afe634-8778-405b-9737-ae95f28e833c']],'ports':[[0]],'algorithm':'SqlExprExecute','outputs':[]},{'nodeid_in_workflow':'f2f63b6f-76fe-4ec4-aa33-310138e4514b','component_id':'2946c261700747e18c709ee955a74857','params':{'sql_expr':'select max(id) as id, max(host) as host,max(J_0001_00_247) as J_0001_00_247 from t2 group by id','table_name':'t2','returns':[]},'requirements_id':[['256363a0-4dee-4436-96ad-405dde910d8e']],'ports':[[0]],'algorithm':'SqlExprExecute','outputs':[]},{'nodeid_in_workflow':'213c4114-47c9-44c6-aa56-a9baa21505a1','component_id':'2946c261700747e18c709ee955a74857','params':{'sql_expr':'select max(id) as id, max(host) as host,max(J_0001_00_247) as J_0001_00_247  from t3 group by id','table_name':'t3','returns':[]},'requirements_id':[['f2f63b6f-76fe-4ec4-aa33-310138e4514b']],'ports':[[0]],'algorithm':'SqlExprExecute','outputs':['test://123456789/wq/flok/test_result.csv']}]";
    private static String jarPath = "file:///Users/wangqi/Downloads/workflow-0.1.0-SNAPSHOT-jar-with-dependencies.jar";
    private static String jarPath_server  = "file:///tmp/spark-warehouse/user_defined/algorithm/workflow-0.1.0-SNAPSHOT-jar-with-dependencies.jar";
    private static int threadNum = 5;
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        SparkSession session = SparkSession.builder().master(masterUrl).getOrCreate();
        SparkStreamSchedular streamShcedular = new SparkStreamSchedular(session,jarPath_server,definition,12);
        streamShcedular.batchCount = Integer.parseInt(args[0]);
        streamShcedular.start();
        long end = System.currentTimeMillis();
        timeLog.add("总时间：:"+((end-start)/1000)+"");
        //System.out.println("time:"+);
        for(String line:timeLog){
            System.out.println(line);
        }
    }
}
