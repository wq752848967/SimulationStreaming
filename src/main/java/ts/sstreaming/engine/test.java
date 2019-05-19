package ts.sstreaming.engine;

import org.json.JSONArray;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;

public class test {
    public static void main(String[] args) {
//        String json = "[{'nodeid_in_workflow':'71390cea-53a1-4979-b7ec-c736ba11ad1a','component_id':'34b8de51c05141c8af20210f4c7eec71','params':{'path':'hdfs://192.168.10.12:9000/flok/task4.csv','delimiter':',','header_text':'','returns':[]},'requirements_id':[],'ports':[],'algorithm':'ReadHdfsOper'},{'nodeid_in_workflow':'50877668-1b06-4ac0-93ec-fd6ca69ee9a9','component_id':'34b8de51c05141c8af20210f4c7eec71',\n" +
//                "'params':{'path':'hdfs://192.168.10.12:9000/flok/task4_new.csv','delimiter':',','header_text':'','returns':[]},'requirements_id':[],'ports':[],'algorithm':'ReadHdfsOper'},{'nodeid_in_workflow':'157bc1b2-ab2e-4d00-b8f9-8ef3e0fe47f3',\n" +
//                "'component_id':'46cce27abff54e6a98e21965511e58da',\n" +
//                "'params':{'sql_expr':'select t1.col1,col2,output,col2_new,output_new from t1 left outer join t2 on t1.col1=t2.col1','table_name':'t1','another_table_name':'t2','returns':[]},\n" +
//                "'requirements_id':[['71390cea-53a1-4979-b7ec-c736ba11ad1a'],['50877668-1b06-4ac0-93ec-fd6ca69ee9a9']],\n" +
//                "'ports':[[0],[0]],'algorithm':'SqlExecuteForTables'},{'nodeid_in_workflow':'47f71e07-0546-4b0d-870c-5546866e15d6',\n" +
//                "'component_id':'b97a01003c3644a19a16d4015b327f1f','params':{'left_rate':'0.5','right_rate':'0.5','returns':[]},'requirements_id':[['157bc1b2-ab2e-4d00-b8f9-8ef3e0fe47f3']],'ports':[[0]],'algorithm':'Split'},{'nodeid_in_workflow':'c979b514-d1f9-4a0a-b33f-1ff8a817e7ea','component_id':'2946c261700747e18c709ee955a74857','params':{'sql_expr':'select col1 from t','table_name':'t','returns':[]},'requirements_id':[['47f71e07-0546-4b0d-870c-5546866e15d6']],'ports':[[0]],'algorithm':'SqlExprExecute'},{'nodeid_in_workflow':'680ce594-1a79-4fed-b296-d3c8da589225','component_id':'2946c261700747e18c709ee955a74857','params':{'sql_expr':'select col2 from t','table_name':'t','returns':[]},'requirements_id':[['47f71e07-0546-4b0d-870c-5546866e15d6']],'ports':[[1]],'algorithm':'SqlExprExecute'}\n" +
//                "]";
//        SparkStreamSchedular schedular = new SparkStreamSchedular("","",json);
//        schedular.start();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        System.out.println(df.format(new Date()));// new Date()为获取当前系统时间

    }
}
