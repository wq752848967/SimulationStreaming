package ts.workflow.lib;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class ParquetWriter implements FloKDataWriter {

    final static Logger logger = Logger.getLogger(ParquetWriter.class);

    @Override
    public void writeData(String outputPath, Dataset<Row> outData, SparkSession sparkSession) {
        outData.write()
                .mode(SaveMode.Overwrite)
                .option("header","true")
                .format("parquet")
                .save(outputPath);
        logger.info("Write dataframe to output path :" + outputPath);
    }

    public void writeData(String outputPath, Dataset<Row> outData, SparkSession sparkSession, String delimiter) {
        outData.write()
                .mode(SaveMode.Overwrite)
                .option("header","true")
                .option("delimiter", delimiter)
                .format("parquet")
                .save(outputPath);
        logger.info("Write dataframe to output path :" + outputPath);
    }
}
