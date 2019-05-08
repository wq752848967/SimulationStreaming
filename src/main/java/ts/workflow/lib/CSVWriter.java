package ts.workflow.lib;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVWriter implements FloKDataWriter{

    final static Logger logger = Logger.getLogger(CSVWriter.class);

    @Override
    public void writeData(String outputPath, Dataset<Row> outData, SparkSession sparkSession) {
//        if(outData == null) {
//            logger.error("write null data!");
//            return;
//        }
//        else if(outData.rdd().isEmpty()){
//            String[] fieldnames = outData.schema().fieldNames();
//            List<String[]> datas = new ArrayList<>();
//            datas.add(fieldnames);
//	    logger.info("#############dataset is empty.");
//            Dataset<Row> dataFrame = DataFormatTool.list2Dataframe(datas, Arrays.asList(fieldnames), StructFieldTypes.String, sparkSession);
////            dataFrame.write().mode(SaveMode.Overwrite)
////                    .format("com.databricks.spark.csv")
////                    .option("header", "false")
////                    .save(outputPath);
//            dataFrame.write().mode(SaveMode.Overwrite).option("header","false").csv(outputPath);
//            logger.info("Get empty dataframe. Write header to " + outputPath);
//        }
//        else {
////            outData.write().mode(SaveMode.Overwrite)
////                    .format("com.databricks.spark.csv")
////                    .option("header", "true")
////                    .save(outputPath);
//            outData.write().mode(SaveMode.Overwrite).option("header","true").csv((outputPath));
//            logger.info("Write dataframe to output path :" + outputPath);
  //      }
    }

    public void writeData(String outputPath, Dataset<Row> outData, SparkSession sparkSession, String delimiter) {
//        if(outData == null) {
//            logger.error("write null data!");
//            return;
//        }
//        else if(outData.rdd().isEmpty()){
//            outData.show();
//            String[] fieldnames = outData.schema().fieldNames();
//            List<String[]> datas = new ArrayList<>();
//            datas.add(fieldnames);
//            logger.info("#############dataset is empty.");
//            Dataset<Row> dataFrame = DataFormatTool.list2Dataframe(datas, Arrays.asList(fieldnames), StructFieldTypes.String, sparkSession);
//            dataFrame.write().mode(SaveMode.Overwrite).option("header","false").option("delimiter", delimiter).csv(outputPath);
//            logger.info("Get empty dataframe. Write header to " + outputPath);
//        }
//        else {
//            outData.write().mode(SaveMode.Overwrite).option("header","true").option("delimiter", delimiter).csv((outputPath));
//            logger.info("Write dataframe to output path :" + outputPath);
//        }
    }
}
