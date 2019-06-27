package ts.hg_sstreaming.test.utils.DataSplit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DataCreate {
    public static void main(String[] args) {
        SparkSession session_java = SparkSession.builder().master("spark://172.16.244.8:7077").getOrCreate();
        Dataset<Row> ds = session_java.read().option("header","true").option("delimiter",",").csv("hdfs://172.16.244.5:9000/flok/sim/sim_test_small.csv");
//        ds.show();
        int count =Integer.parseInt(args[0]);
        for(int i=1;i<count;i++){
            //ds
            ds = ds.union(ds);
        }
        ds.coalesce(1);
        ds.write().mode(SaveMode.Overwrite).option("header","true").option("delimiter",",").csv(args[1]);

    }
}
