package ts.workflow.lib;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface FloKDataReader {

    Dataset<Row> readData(String inputPath, SparkSession sparkSession);
    // TODO: read model method

}
