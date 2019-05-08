package ts.workflow.lib;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class FloKDataSet {

    final static Logger logger = Logger.getLogger(FloKDataSet.class);

    private List<Dataset<Row>> dfList;
    private int counter = 0;

    // model path list
    private List<String> modelInputPath;

    private List<String> modelOutputPath;

    public FloKDataSet() {
        this.dfList = new ArrayList<>();
        this.modelInputPath = new ArrayList<>();
        this.modelOutputPath = new ArrayList<>();
    }

    public int getSize() {
        if (dfList == null){
            logger.info("DataSet is empty!");
            return 0;
        }
        else
            logger.info("FloKData size:" + dfList.size());
        return dfList.size();
    }

    public List<Dataset<Row>> getData() {
        if (dfList == null){
            logger.info("DataSet is empty!");
        }
        return dfList;
    }

    public Dataset<Row> get(int index) {
        if(index >= getSize()) {
            logger.error("FloKDataSet out of index:" + index);
        }
        return dfList.get(index);
    }

    public void addDF(Dataset<Row> newdf) {
        dfList.add(newdf);
    }

    public void addModelInputPath(String path) {
        modelInputPath.add(path);
    }

    public void addModelOutputPath(String path) {
        modelOutputPath.add(path);
    }


    public String getModelInputPath(int index) {
        if(index >= modelInputPath.size()) {
            logger.error("FloKDataSet's model input paths list out of index:" + index);
        }
        return modelInputPath.get(index);
    }
    public String getModelOutputPath(int index) {
        if(index >= modelOutputPath.size()) {
            logger.error("FloKDataSet's model output paths list out of index:" + index);
        }
        return modelOutputPath.get(index);
    }

    public int getModelSize() {
        return  modelOutputPath.size();
    }

    protected Dataset<Row> next() {
        if (counter < dfList.size())
            return dfList.get(counter++);
        else
            return null;
    }
}
