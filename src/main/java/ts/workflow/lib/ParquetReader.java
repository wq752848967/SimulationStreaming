package ts.workflow.lib;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetReader implements FloKDataReader {

    final static Logger logger = Logger.getLogger(ParquetReader.class);

    @Override
    public Dataset<Row> readData(String inputPath, SparkSession sparkSession) {
        Dataset<Row> dataframe = sparkSession.read()
                .option("header","true")
                .format("parquet")
                .parquet(inputPath);
        logger.info("Get dataframe from input :" + inputPath);
        return dataframe;
    }

    public Dataset<Row> readData(String inputPath, SparkSession sparkSession, String delimiter) {
        Dataset<Row> dataframe = sparkSession.read()
                .option("header","true")
                .option("delimiter", delimiter)
                .format("parquet")
                .parquet(inputPath);
        logger.info("Get dataframe from input :" + inputPath);
        return dataframe;
    }
}
