package ts.sstreaming.engine;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import ts.sstreaming.pojos.FlokAlgNode;
import ts.sstreaming.utils.impl.JarObjectLoaderImpl;
import ts.sstreaming.utils.impl.SparkDataSplitImpl;
import ts.sstreaming.utils.inter.DataSplitInter;
import ts.sstreaming.utils.inter.ObjectLoaderInter;
import ts.workflow.lib.FloKAlgorithm;
import ts.workflow.lib.FloKDataSet;

import java.util.*;


public class SparkStreamSchedular {
    /**
     *
     *  创建每个node 实例节点
     *  构建拓扑关系11
     *  原始数据切分
     *  多线程调用
     *
     *
     */

   private SparkSession session = null;
   private int batchCount  = 5;
   private String jarPath = "";
   private String definition =  "";
   private ObjectLoaderInter classloader = null;
   private List<FlokAlgNode> headers = new ArrayList<>();
   private DataSplitInter dataSpliter = null;
   private HashMap<String,FlokAlgNode> nodeId_algNode = new HashMap<>();

    public SparkStreamSchedular(SparkSession session, String jarPath, String definition) {
        this.session = session;
        this.jarPath = jarPath;
        this.definition = definition;
    }


    public void start(){

        /**
         * 实例化类加载器
          */
        classloader = new JarObjectLoaderImpl();


        /**
         * 解析definition
         */

        JSONArray jsonArray = new JSONArray(definition);
        createNodes(jsonArray);
        buildDency(jsonArray);




        /**
         * 切分数据，并进行数据push
         */
        dataSpliter = new SparkDataSplitImpl(session);
        for(FlokAlgNode node:headers){
           /**
            * !!!!!!!  warning  !!!!!!!
            * 多线程跑数据进行切分
            * 此处暂时采用单线程
            */
           FloKDataSet dataset = node.runAlg(true);
           Dataset<Row>[] input_arr = dataSpliter.splitDataByBatch(dataset.get(0),batchCount);
           for(String nextNodeId :node.getDency().get("0")){
               for(Dataset<Row> ds:input_arr){
                   //nextNodeId 需要进行保存
                   nodeId_algNode.get(nextNodeId).getData_queue().offer(ds);
               }
           }

        }


        /**
         *
         * 多线程部分
         *
         */




    }

    /**
     * 解析节点，但是不构建依赖关系
     *
     */
    public void createNodes(JSONArray jsonArray){

        String component_id = "";
        String nodeid_in_workflow = "";
        String algorithm  = "";
        for(int i=0;i<jsonArray .length();i++){
            //遍历所有node节点
            JSONObject jsonObject = jsonArray .getJSONObject(i);
            component_id= jsonObject.getString("component_id");
            nodeid_in_workflow = jsonObject.getString("nodeid_in_workflow");
            algorithm = "ts.workflow.operator."+jsonObject.getString("algorithm");
            HashMap<String,String> param = new HashMap<>();
            JSONObject sub_jsonObj = jsonObject.getJSONObject("params");
            Map<String,Object> p_map = sub_jsonObj.toMap();
            for (String key:p_map.keySet()){
                String val = p_map.get(key).toString();
                param.put(key,val);
            }
            /**
             * 传入masterUrl应用于初始化sparkSession
             */
            FloKAlgorithm alg = (FloKAlgorithm)classloader.loadObject(jarPath,algorithm,session);
            FlokAlgNode algNode = new FlokAlgNode(alg,algorithm,component_id,nodeid_in_workflow,param);
            nodeId_algNode.put(nodeid_in_workflow,algNode);
            //nodeId_algNode.put(nodeid_in_workflow,new FlokAlgNode(null,algorithm,component_id,nodeid_in_workflow,param));
        }
    }

    /**
     *
     * 在所拥有对象已经创建的基础上
     * 构建依赖关系
     *
     *
     */
    public void buildDency(JSONArray jsonArray){
        Map<String,Boolean> in_count = new HashMap<>();
        for(int i=0;i<jsonArray .length();i++) {
            //遍历所有node节点
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String node_in_work = jsonObject.getString("nodeid_in_workflow");
            JSONArray arr_dency = jsonObject.getJSONArray("requirements_id");
            JSONArray arr_port = jsonObject.getJSONArray("ports");
            in_count.put(node_in_work,true);
            /**
             * 遍历依赖
             */
            //[[]]
            for(int j=0;j<arr_dency .length();j++) {
                //[]
                in_count.put(node_in_work,false);
                JSONArray node_arr = arr_dency.getJSONArray(j);
                JSONArray port_arr = arr_port.getJSONArray(j);
                for (int k = 0; k <node_arr.length(); k++) {

                    String node_id = node_arr.get(k).toString();
                    String port_index = port_arr.get(k).toString();
                    //System.out.println("node_in_work:"+node_in_work+"  "+node_id);
                    if(!nodeId_algNode.containsKey(node_id)){
                        System.err.println("not exist:"+nodeId_algNode);
                    }else{
                        Map<String, List<String>> den = nodeId_algNode.get(node_id).getDency();
                        if(den.containsKey(port_index)){
                            den.get(port_index).add(node_in_work);
                        }else{
                            List<String> denList = new ArrayList<>();
                            denList.add(node_in_work);
                            den.put(port_index,denList);
                        }
                    }

                }
            }

        }
        for(String key:in_count.keySet()){
            if(in_count.get(key)){
                headers.add(nodeId_algNode.get(key));
            }
        }

/**
 * 依赖构建测试代码
 */

        //        String s =  "47f71e07-0546-4b0d-870c-5546866e15d6";
//        for(String key:nodeId_algNode.get(s).getDency().keySet()){
//
//            System.out.println(nodeId_algNode.get(s).getDency().size()+" ");
//            System.out.print(key);
//            System.out.print("  ");
//            System.out.println(nodeId_algNode.get(s).getDency().get(key));
//        }
    }
}
