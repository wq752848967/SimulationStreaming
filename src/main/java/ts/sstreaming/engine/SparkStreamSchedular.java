package ts.sstreaming.engine;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ts.sstreaming.main.FlokSimStreamContext;
import ts.sstreaming.main.FlokSimStreamTest;
import ts.sstreaming.pojos.FLokAlgNodeStatus;
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


public class SparkStreamSchedular{

    /**
     *
     *  创建每个node 实例节点
     *  构建拓扑关系11
     *  原始数据切分
     *  多线程调用
     *
     *
     */
    private static Logger LOGGER = LoggerFactory.getLogger(SparkStreamSchedular.class);
    private static volatile boolean flag = true;
   private SparkSession session = null;
   private int threadNum = 5;
   private long start_time = 0;
   private long end_time = 0;
   public int batchCount  = 5;
   private AtomicInteger runningNodeCount = new AtomicInteger();
   private String jarPath = "";
   private String definition =  "";
   private int node_instance_id = 0;
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
    public SparkStreamSchedular(SparkSession session, String jarPath, String definition,int threadNum,int batchCount) {
        this.session = session;
        this.jarPath = jarPath;
        this.definition = definition;
        this.threadNum = threadNum;
        this.batchCount = batchCount;
    }

    public void start(){

        /**
         * 实例化类加载器
          */
        classloader = new JarObjectLoaderImpl();


        /**
         * 解析definition
         */

        start_time = System.currentTimeMillis();
        LOGGER.info("解析definition start ......");
        JSONArray jsonArray = new JSONArray(definition);
        try{
            createNodes(jsonArray);
            buildDency(jsonArray);
        }catch (Exception e){
            e.printStackTrace();
        }
        LOGGER.info("解析definition end ......");
        end_time = System.currentTimeMillis();
        FlokSimStreamTest.timeLog.add(("解析definition :"+(end_time-start_time)/1000)+"");


        /**
         * 切分数据，并进行数据push
         */
        start_time = System.currentTimeMillis();
        LOGGER.info("切分数据 start ......");
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
        LOGGER.info("切分数据 end ......");
        end_time = System.currentTimeMillis();
        FlokSimStreamTest.timeLog.add(("切分数据 :"+(end_time-start_time)/1000)+"");

        /**
         *
         * 多线程部分
         *
         */
        start_time = System.currentTimeMillis();
        LOGGER.info("task execute start ......");
        ExecutorService threadPool = Executors.newFixedThreadPool(this.threadNum);
        for(int i=0;i<threadNum;i++){
            threadPool.execute(new MessageLoop());
        }

        //如何终止是个问题
        //终止的条件：无任务在执行，并且taskqueue中为空
        while(!checkStreamStatus()){
           try{
               Thread.sleep(3000);
           }catch (InterruptedException ie){
               ie.printStackTrace();
           }

        }
        LOGGER.info("task execute end ......");
        end_time = System.currentTimeMillis();
        FlokSimStreamTest.timeLog.add(("数据片执行 :"+(end_time-start_time)/1000)+"");
        /**
         *
         * 关闭线程池
         *
         */
        flag = false;
        threadPool.shutdown();
        threadPool.shutdownNow();

        /**
         *
         * 合并结果数据
         *
         */
        start_time = System.currentTimeMillis();
        for(FlokAlgNode node:nodeId_algNode.values()){
            //node.outputData(111);
            //node.flushOutData();
            node.printOutData();
        }
        end_time = System.currentTimeMillis();
        FlokSimStreamTest.timeLog.add(("合并数据 :"+(end_time-start_time)/1000)+"");
        //finish
        System.out.println("is finish OK :"+threadPool.isShutdown()+" "+threadPool.isTerminated());
        return;
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
    public void createNodes(JSONArray jsonArray) throws Exception{

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
            //System.out.println(nodeid_in_workflow+"   "+arr_out.length());
            for (int j = 0; j <arr_outputs.length; j++) {


                arr_outputs[j] = arr_out.get(j).toString();
            }
            /**
             * 传入masterUrl应用于初始化sparkSession
             */
            FloKAlgorithm alg = (FloKAlgorithm)classloader.loadObject(jarPath,algorithm,session);
            if(alg==null){
                throw  new Exception("class load err:"+jarPath+" "+algorithm);
            }
            FlokAlgNode algNode = new FlokAlgNode(alg,algorithm+"_"+node_instance_id,component_id,nodeid_in_workflow,param,arr_outputs);
            nodeId_algNode.put(nodeid_in_workflow,algNode);
            node_instance_id++;
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
            for(int j=0;j<arr_dency.length();j++) {
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
//        System.out.println("::************  node info:");
//        System.out.println(""+nodeId_algNode.size());
//        for(String nodeId:nodeId_algNode.keySet()){
//            System.out.println(nodeId);
//            FlokAlgNode node = nodeId_algNode.get(nodeId);
//            for(List<String> list:node.getDency().values()){
//                for(String den:list){
//                    System.out.print("*");
//                    System.out.print(den);
//                    System.out.print("*   ");
//                }
//                System.out.println("");
//            }
//        }

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
            while(flag){
                //获取任务
                String nodeAlg_id = taskQueue.poll();
                if(nodeAlg_id==null){
                    continue;
                }
                if(nodeId_algNode.get(nodeAlg_id).getStatus()== FLokAlgNodeStatus.NodeStatus.RUNNING){
                    taskQueue.offer(nodeAlg_id);
                    continue;
                }
                //终止代码判断
                runningNodeCount.incrementAndGet();
                FlokAlgNode nodeAlg = nodeId_algNode.get(nodeAlg_id);

                FloKDataSet fds = nodeAlg.runAlg(false);



                //向下分发数据
                if(fds.getSize()>0){
                    //fds.get(0).count();
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

