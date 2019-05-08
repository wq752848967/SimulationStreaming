package ts.workflow.lib;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.List;


abstract public class FloKAlgorithm {

    final static Logger logger = Logger.getLogger(FloKAlgorithm.class);

    public SparkSession sparkSession;

    public FloKAlgorithm(){}

    /**
     * This method need to be override if you need to set some SparkSession configurations
     * e.g. SparkSession.builder().config("spark.cassandra.connection.host", "0.0.0.0").getOrCreate();
     * @param arguments
     */
    public void initAlgorithm(FloKAlgorithmArguments arguments) {
        // config for log4j
        //org.apache.log4j.BasicConfigurator.configure(new NullAppender());
        if(arguments.getMasterUrl()!=null&&arguments.getMasterUrl().length()!=0){
            this.sparkSession = SparkSession.builder().master(arguments.getMasterUrl()).appName(arguments.mainclass).getOrCreate();
        }else{
            this.sparkSession = SparkSession.builder().master("local[2]").appName(arguments.mainclass).getOrCreate();
        }

    }

    /**
     * User need to implement this method
     * @param inputDataSets
     * @return
     */
    public abstract FloKDataSet run(FloKDataSet inputDataSets, HashMap<String, String> params);

    private static FloKAlgorithmArguments parseParam(String json) {
        Gson gson = new Gson();
        logger.info("Json input: " + json);
        FloKAlgorithmArguments arguments = gson.fromJson(json, FloKAlgorithmArguments.class);
        System.out.println(arguments.toString());
        //HashMap t = arguments.parameters.get(0);
        logger.info("Get argments for algorithm");
        return arguments;
    }

    /**
     * Read data with type(CSV, Parquet, Model)
     * @param inputPath
     * @param inputType
     * @return
     */
    public FloKDataSet read(List<String> inputPath, List<String> inputType,
                            List<String> outputPath, List<String> outputType) {
//        if (inputPath.size() != inputType.size()){
//            logger.error("InputType's number is not equals to input's number");
//            return null;
//        }
        String globalInputType = null;
        if (inputPath.size() != inputType.size()) {
            if (inputType.size() >= 1)
                globalInputType = inputType.get(0);
            else {
                logger.error("InputType length need to be equal with InputPath length, or just set one input type");
            }
        }
        FloKDataSet dataset = new FloKDataSet();
        int len = inputPath.size();
        for(int i = 0; i < len; i++) {
            Dataset<Row> data = null;
            switch (globalInputType == null? inputType.get(i) : globalInputType) {
                case DataTypes.CSV: {
                    data = new CSVReader().readData(inputPath.get(i), sparkSession, "|");
                } break;
                case DataTypes.PARQUET: {
                    data = new ParquetReader().readData(inputPath.get(i), sparkSession, "|");
                } break;
                case DataTypes.MODEL: {
                    dataset.addModelInputPath(inputPath.get(i));
                } continue;
            }
            dataset.addDF(data);
        }
        len = outputPath.size();
        for(int i = 0; i < len; i++) {
            System.out.println(outputPath.get(i));
            if (outputType.get(i).equals(DataTypes.MODEL)) {
                dataset.addModelOutputPath(outputPath.get(i));
            }
        }
        return dataset;
    }

    /**
     * Write data without return.
     * @param outputPath
     * @param outData
     * @param outputType
     */
    public void write(List<String> outputPath, FloKDataSet outData, List<String> outputType) {

        if (outData == null) {
            logger.info("Algorithm without output data");
            return;
        }

        if (outputPath.size() != (outData.getSize() + outData.getModelSize()) ||
                outputPath.size() != outputType.size()) {
            logger.error("OutData's number is not equals to output's number");
        }

        int len = outputPath.size();
        for(int i = 0; i < len; i++) {
            switch (outputType.get(i)) {
                case DataTypes.CSV: {
                    new CSVWriter().writeData(outputPath.get(i), outData.next(), sparkSession, "|");
                } break;
                case DataTypes.PARQUET: {
                    new ParquetWriter().writeData(outputPath.get(i), outData.next(), sparkSession, "|");
                } break;
                case DataTypes.MODEL: {
                    // TODO: How to manage model?
                    // Need to be implemented in user class
                } break;
            }
        }
    }

    public void writeData() {
        // If you want write data to other storage
        // override this method.
    }


    public static void main(String[] args) {

        if(args.length < 1) {
            logger.error("Missing parameters for algorithm");
            return;
        }

        FloKAlgorithmArguments floKAlgorithmArgus = parseParam(args[0]);
        Class c = null;
        try {
            logger.info("Get algorithm class name:" + floKAlgorithmArgus.mainclass);
            c = Class.forName(floKAlgorithmArgus.mainclass);
        } catch (ClassNotFoundException e) {
            logger.error("Class " + floKAlgorithmArgus.mainclass + "not found");
            e.printStackTrace();
        }
        FloKAlgorithm algorithm = null;
        try {
            algorithm = (FloKAlgorithm)c.newInstance();
            algorithm.initAlgorithm(floKAlgorithmArgus);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        //TODO: There need to have a check.
        FloKDataSet dataSet = algorithm.read(floKAlgorithmArgus.input, floKAlgorithmArgus.inputType,
                floKAlgorithmArgus.output, floKAlgorithmArgus.outputType);
        FloKDataSet result = algorithm.run(dataSet, floKAlgorithmArgus.parameters);
        algorithm.write(floKAlgorithmArgus.output, result, floKAlgorithmArgus.outputType);

        System.out.println("Algorithm output data write out. timestamp:" + System.currentTimeMillis());
        if (algorithm.sparkSession != null)
            algorithm.sparkSession.stop();
        System.out.println("FloK task finished!");
        System.exit(0);
        System.out.println("Stop successfully.");
    }
}
