package ts.sstreaming.utils.inter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataSplitInter {
    public  Dataset<Row>[] splitDataByCount(Dataset<Row> ds,int count);
    public  Dataset<Row>[] splitDataByTimeWindows(Dataset<Row> ds,Long time_interval);

}
