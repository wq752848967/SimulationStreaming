package ts.hg_sstreaming.test;

import org.apache.spark.sql.SparkSession;

public class Iso_SDriver {
    private static String masterUrl = "local[5]";
    private final static String JAVA_OUT_PATH = "hdfs://192.168.35.55:9000/flok/sim/java_out";
    private final static String SPARK_OUT_PATH = "hdfs://192.168.35.55:9000/flok/sim/spark_out";
    private final static String PYHTON_OUT_PATH = "hdfs://192.168.35.55:9000/flok/sim/python_out";
    private final static String RIGHT_PATH = "hdfs://192.168.35.55:9000/flok/4665/csv_loader-1530083012_dafde4f6-9eda-404a-87df-c2fc51bf0dab_0.output";
    public static void main(String[] args) {
        /**
         * 初始化数据
         */
        //初始化数据

        //初始化session
        SparkSession session_java = SparkSession.builder().master(masterUrl).getOrCreate();
        SparkSession session_spark = SparkSession.builder().master(masterUrl).getOrCreate();

        //初始化计算算子
        Iso_SJavaOp javaOp = new Iso_SJavaOp(session_java,JAVA_OUT_PATH,RIGHT_PATH);
        Iso_SSparkOp sparkOp = new Iso_SSparkOp(session_spark,SPARK_OUT_PATH,RIGHT_PATH);


        //构建算子流程关系
        javaOp.setOutQueue(sparkOp.getInQueue());

        //算子初始化
        javaOp.init();
        sparkOp.init();



        /**
        *
        * 唤起消费线程
         *
        * */
        Thread javaConsumerThread =  new Thread(javaOp);
        Thread sparkConsumerThread =  new Thread(sparkOp);


        //启动
        javaConsumerThread.start();
        sparkConsumerThread.start();

    }
}
