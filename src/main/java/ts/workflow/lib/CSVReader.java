package ts.workflow.lib;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVReader implements FloKDataReader {

    final static Logger logger = Logger.getLogger(CSVReader.class);

    @Override
    public Dataset<Row> readData(String inputPath, SparkSession sparkSession) {
//        Dataset<Row> dataframe = sparkSession.read()
//                .format("com.databricks.spark.csv")
//                .option("header", "true") //reading the headers
//                .option("mode", "DROPMALFORMED")
//                .load(inputPath);

        Dataset<Row> dataframe = sparkSession.read().option("header",true).csv(inputPath);
        logger.info("Get dataframe from input :" + inputPath);
        return dataframe;
    }

    public Dataset<Row> readData(String inputPath, SparkSession sparkSession, String delimiter) {
        Dataset<Row> dataframe = sparkSession.read().option("header",true).option("delimiter", delimiter).csv(inputPath);
        logger.info("Get dataframe from input :" + inputPath);
        return dataframe;
    }
}
