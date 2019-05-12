package ts.sstreaming.main;

import org.apache.spark.sql.SparkSession;
import ts.sstreaming.engine.SparkStreamSchedular;

public class FlokSimStreamTest {
    private static String masterUrl = "local[2]";
    private static String definition = "[{'nodeid_in_workflow':'d01a22b1-e429-458a-b1d1-c7046d18a792','component_id':'34b8de51c05141c8af20210f4c7eec71','params':{'path':'hdfs://192.168.10.12:9000/flok/layer1_35all_J247.csv','delimiter':',','header_text':'','returns':[]},'requirements_id':[],'ports':[],'outputs':[],'algorithm':'HDFSReader'},{'nodeid_in_workflow':'ecd44ba3-2593-429c-b6a6-113090bce31e','component_id':'01ae2cb1ac32437d823a34ff55373876','params':{'attr_names':'host,align_time','returns':[]},'requirements_id':[['d01a22b1-e429-458a-b1d1-c7046d18a792']],'ports':[[0]],'outputs':[],'algorithm':'SelectAttribute'},{'nodeid_in_workflow':'1fa29777-cc43-40da-b8ba-92d9dad54a3f','component_id':'0ba7d71b30e24759b6200b3a615f57c6','params':{'attr_names':'host'},'requirements_id':[['ecd44ba3-2593-429c-b6a6-113090bce31e']],'ports':[[0]],'outputs':['test://123456789/wq/flok/test_result.csv'],'algorithm':'SelectAttribute'}]";
    private static String jarPath = "file:///Users/wangqi/Downloads/workflow-0.1.0-SNAPSHOT-jar-with-dependencies.jar";
    private static int threadNum = 5;
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().master(masterUrl).getOrCreate();
        SparkStreamSchedular streamShcedular = new SparkStreamSchedular(session,jarPath,definition,2);
        streamShcedular.start();
    }
}
