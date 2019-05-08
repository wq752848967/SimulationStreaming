package ts.sstreaming.pojos;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ts.workflow.lib.FloKAlgorithm;
import ts.workflow.lib.FloKDataSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class FlokAlgNode {
    private FloKAlgorithm flokAlg = null;
    private String className = "";
    private String component_id = "";
    private String nodeid_in_workflow = "";
    private LinkedBlockingQueue<Dataset<Row>> data_queue = new LinkedBlockingQueue<>();
    private HashMap<String,String> params = null;
    private FloKDataSet flokDataset = new FloKDataSet();
    private Map<String, List<String>> dency = new HashMap<>();
    public FlokAlgNode(FloKAlgorithm flokAlg, String className, String component_id, String nodeid_in_workflow, HashMap<String, String> params) {
        this.flokAlg = flokAlg;
        this.className = className;
        this.component_id = component_id;
        this.nodeid_in_workflow = nodeid_in_workflow;
        this.params = params;
    }


    public FloKDataSet runAlg(boolean isHead){
        FloKDataSet result = null;
        if(isHead){
            result =  flokAlg.run(new FloKDataSet(),params);
        }
       return result;
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
