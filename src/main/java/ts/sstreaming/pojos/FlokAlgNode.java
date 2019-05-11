package ts.sstreaming.pojos;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ts.workflow.lib.FloKAlgorithm;
import ts.workflow.lib.FloKDataSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class FlokAlgNode {

    private FloKAlgorithm flokAlg = null;
    private String className = "";
    private volatile FLokAlgNodeStatus.NodeStatus status = FLokAlgNodeStatus.NodeStatus.WAIT;
    private String component_id = "";
    private String nodeid_in_workflow = "";
    private LinkedBlockingQueue<Dataset<Row>> data_queue = new LinkedBlockingQueue<>();
    private HashMap<String,String> params = null;
    private FloKDataSet flokDataset = new FloKDataSet();
    private FloKDataSet result = null;
    private String[] outputs = null;
    private Map<String, List<String>> dency = new HashMap<>();
    private Map<String, List<Dataset<Row>>> resultSplit = new HashMap<>();
    private boolean isEndingNode  = false;
    public FlokAlgNode(FloKAlgorithm flokAlg, String className, String component_id, String nodeid_in_workflow, HashMap<String, String> params) {
        this.flokAlg = flokAlg;
        this.className = className;
        this.component_id = component_id;
        this.nodeid_in_workflow = nodeid_in_workflow;
        this.params = params;
    }
    public FlokAlgNode(FloKAlgorithm flokAlg, String className, String component_id, String nodeid_in_workflow, HashMap<String, String> params,String[] outputs) {
        this.flokAlg = flokAlg;
        this.className = className;
        this.component_id = component_id;
        this.nodeid_in_workflow = nodeid_in_workflow;
        this.params = params;
        this.outputs = outputs;
    }

    /**
     * 调用run方法
     * @param isHead
     * @return
     */
    public FloKDataSet runAlg(boolean isHead){
        status = FLokAlgNodeStatus.NodeStatus.RUNNING;

        if(isHead){
            result =  flokAlg.run(new FloKDataSet(),params);
        }else{
            flokDataset.clearDF();
            Dataset<Row> data = data_queue.poll();
            flokDataset.addDF(data);
            result = flokAlg.run(flokDataset,params);
            if(isEndingNode){
                //尾节点数据处理
                outputData();
                result.clearDF();
            }

        }
        status = FLokAlgNodeStatus.NodeStatus.WAIT;
       return result;
    }


    /**
     * 输出结果
     */
    public void outputData(){
        if(isEndingNode&&outputs!=null){
            for(int i=0;i<outputs.length;i++){
                String path = outputs[i];
                if(resultSplit.containsKey(path)){
                    resultSplit.get(path).add(result.get(i));
                }else{
                    List<Dataset<Row>> list =  new ArrayList<>();
                    list.add(result.get(i));
                    resultSplit.put(path,list);
                }
            }
        }
    }

    public void flushOutData(){
        //resultSplit
    }


    /**
     *
     * getter and setter
     *
     * @return
     */
    public FLokAlgNodeStatus.NodeStatus getStatus() {
        return status;
    }

    public void setStatus(FLokAlgNodeStatus.NodeStatus status) {
        this.status = status;
    }

    public boolean isEndingNode() {
        return isEndingNode;
    }

    public void setEndingNode(boolean endingNode) {
        isEndingNode = endingNode;
    }

    public Map<String, List<String>> getDency() {
        return dency;
    }

    public void setDency(Map<String, List<String>> dency) {
        this.dency = dency;
    }

    public String getComponent_id() {
        return component_id;
    }

    public void setComponent_id(String component_id) {
        this.component_id = component_id;
    }

    public String getNodeid_in_workflow() {
        return nodeid_in_workflow;
    }

    public void setNodeid_in_workflow(String nodeid_in_workflow) {
        this.nodeid_in_workflow = nodeid_in_workflow;
    }

    public FloKAlgorithm getFlokAlg() {
        return flokAlg;
    }

    public void setFlokAlg(FloKAlgorithm flokAlg) {
        this.flokAlg = flokAlg;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public LinkedBlockingQueue<Dataset<Row>> getData_queue() {
        return data_queue;
    }

    public void setData_queue(LinkedBlockingQueue<Dataset<Row>> data_queue) {
        this.data_queue = data_queue;
    }
}
