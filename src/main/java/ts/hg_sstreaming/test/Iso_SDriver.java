package ts.hg_sstreaming.test;

import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class Iso_SDriver {
    //private static String masterUrl = "local[1]";
    private static String masterUrl = "spark://192.168.35.55:7077";
    private final static String JAVA_OUT_PATH = "hdfs://192.168.35.55:9000/flok/sim/java_out/";
    private final static String SPARK_OUT_PATH = "hdfs://192.168.35.55:9000/flok/sim/spark_out/";
    private final static String PYHTON_OUT_PATH = "hdfs://192.168.35.55:9000/flok/sim/python_out/";
//    private final static String JAVA_OUT_PATH = "/Users/wangqi/Desktop/FloK/sim/ios/java_out/";
//    private final static String SPARK_OUT_PATH = "/Users/wangqi/Desktop/FloK/sim/ios/spark_out/";
//    private final static String PYHTON_OUT_PATH = "/Users/wangqi/Desktop/FloK/sim/ios/python_out/";
    private final static String RIGHT_PATH = "hdfs://192.168.35.55:9000/flok/4665/csv_loader-1530083012_dafde4f6-9eda-404a-87df-c2fc51bf0dab_0.output";
    //private final static String RIGHT_PATH = "/Users/wangqi/Desktop/FloK/sim/ios/right_data.csv";
    //private final static String RIGHT_PYTHON_PATH = "/Users/wangqi/Desktop/FloK/sim/ios/right_data.csv";
    private final static String RIGHT_PYTHON_PATH = "hdfs://192.168.35.55:9000/flok/sim/right_data_1.csv";
    ///home/flok/data/sim/right_data.csv
    public static void main(String[] args) {
        /**
         * 初始化数据
         */
        //初始化数据
        long start  = System.currentTimeMillis();
        List<String> logs = new ArrayList<String>();
        //初始化session
        SparkSession session_java = SparkSession.builder().master(masterUrl).getOrCreate();
        SparkSession session_spark = SparkSession.builder().master(masterUrl).getOrCreate();

        //初始化计算算子
        Iso_SJavaOp javaOp = new Iso_SJavaOp(session_java,JAVA_OUT_PATH,RIGHT_PATH,logs);
        Iso_SSparkOp sparkOp = new Iso_SSparkOp(session_spark,SPARK_OUT_PATH,RIGHT_PATH,logs);
        Iso_SPython pythonOp = new Iso_SPython(PYHTON_OUT_PATH,RIGHT_PYTHON_PATH,logs);

        //构建算子流程关系
        javaOp.setOutQueue(sparkOp.getInQueue());
        sparkOp.setOutQueue(pythonOp.getInQueue());
        //算子初始化
        javaOp.init();
        sparkOp.init();
        pythonOp.init();


        /**
        *
        * 唤起消费线程
         *
        * */
        Thread javaConsumerThread =  new Thread(javaOp);
        Thread sparkConsumerThread =  new Thread(sparkOp);
        Thread pythonConsumerThread =  new Thread(pythonOp);

        //启动
        javaConsumerThread.start();
        sparkConsumerThread.start();
        pythonConsumerThread.start();

        //填充数据
        javaOp.getInQueue().offer("hdfs://192.168.35.55:9000/flok/sim/sp2/left_1.csv");
        javaOp.getInQueue().offer("hdfs://192.168.35.55:9000/flok/sim/sp2/left_2.csv");
        //javaOp.getInQueue().offer("/Users/wangqi/Desktop/FloK/sim/ios/left_data.csv");
        int input_split_count = 2;
        try {
           while(pythonOp.getFile_index()<input_split_count){
                Thread.sleep(2000);
           }
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("final time cost:"+(end-start)/1000);
        javaOp.stop();
        sparkOp.stop();
        pythonOp.stop();
        logs.stream().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                System.out.println(s);
            }
        });
    }
}
