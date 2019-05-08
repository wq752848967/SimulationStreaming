package ts.workflow.lib;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface FloKDataWriter {

    void writeData(String outputPath, Dataset<Row> outData, SparkSession sparkSession);
    // TODO: write model method

}
