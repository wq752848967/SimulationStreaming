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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


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
   private int threadNum = 5;
   private int batchCount  = 5;
   private AtomicInteger runningNodeCount = new AtomicInteger();
   private String jarPath = "";
   private String definition =  "";
   private ObjectLoaderInter classloader = null;
   private List<FlokAlgNode> headers = new ArrayList<>();
   private DataSplitInter dataSpliter = null;
   private HashMap<String,FlokAlgNode> nodeId_algNode = new HashMap<>();
   private LinkedBlockingQueue<String> taskQueue = new LinkedBlockingQueue<>();
    public SparkStreamSchedular(SparkSession session, String jarPath, String definition,int threadNum) {
        this.session = session;
        this.jarPath = jarPath;
        this.definition = definition;
        this.threadNum = threadNum;
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
                   taskQueue.offer(nextNodeId);
               }
           }

        }


        /**
         *
         * 多线程部分
         *
         */
        ExecutorService threadPool = Executors.newFixedThreadPool(this.threadNum);
        for(int i=0;i<threadNum;i++){
            threadPool.execute(new MessageLoop());
        }

        //如何终止是个问题
        //终止的条件：无任务在执行，并且taskqueue中为空
        while(!checkStreamStatus()){
           try{
               Thread.sleep(30000);
           }catch (InterruptedException ie){
               ie.printStackTrace();
           }

        }

        /**
         *
         * 关闭线程池
         *
         */
        threadPool.shutdown();
        /**
         *
         * 合并结果数据
         *
         */
        for(FlokAlgNode node:nodeId_algNode.values()){
            node.outputData();
            node.flushOutData();
        }

        //finish

    }



    public  boolean checkStreamStatus(){
        if(taskQueue.size()>0){
            return false;
        }
        if(runningNodeCount.get()>0){
            return false;
        }

        return true;
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
            //参数
            HashMap<String,String> param = new HashMap<>();
            JSONObject sub_jsonObj = jsonObject.getJSONObject("params");
            Map<String,Object> p_map = sub_jsonObj.toMap();
            for (String key:p_map.keySet()){
                String val = p_map.get(key).toString();
                param.put(key,val);
            }
            //输出
            JSONArray arr_out = jsonObject.getJSONArray("outputs");
            String[] arr_outputs = new String[arr_out.length()];
            for (int j = 0; j <arr_outputs.length; j++) {
                arr_outputs[i] = arr_out.get(i).toString();
            }
            /**
             * 传入masterUrl应用于初始化sparkSession
             */
            FloKAlgorithm alg = (FloKAlgorithm)classloader.loadObject(jarPath,algorithm,session);
            FlokAlgNode algNode = new FlokAlgNode(alg,algorithm,component_id,nodeid_in_workflow,param,arr_outputs);
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
        //Map<String,Integer> den_count = new HashMap<>();
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
        //header
        for(String key:in_count.keySet()){
            if(in_count.get(key)){
                headers.add(nodeId_algNode.get(key));
            }
        }
        //footer
        for(FlokAlgNode node:nodeId_algNode.values()){
            if(node.getDency().size()==0){
                node.setEndingNode(true);
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

    /**
     *
     * 内部类
     */
    private class MessageLoop implements  Runnable{
        @Override
        public void run() {
            while(true){
                //获取任务
                String nodeAlg_id = taskQueue.poll();
                //终止代码判断
                runningNodeCount.incrementAndGet();
                FlokAlgNode nodeAlg = nodeId_algNode.get(nodeAlg_id);
                FloKDataSet fds = nodeAlg.runAlg(false);

                //向下分发数据
                if(fds.getSize()>0){
                    for(String key:nodeAlg.getDency().keySet()){
                        List<String> den_list_id = nodeAlg.getDency().get(key);
                        Dataset<Row> data_split = fds.get(Integer.parseInt(key));
                        for(String node_id_workflow:den_list_id){
                            nodeId_algNode.get(node_id_workflow).getData_queue().offer(data_split);
                            taskQueue.offer(node_id_workflow);
                        }
                    }
                }
                runningNodeCount.decrementAndGet();

            }
        }
    }
}

