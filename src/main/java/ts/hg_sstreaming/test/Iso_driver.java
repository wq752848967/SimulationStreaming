package ts.hg_sstreaming.test;

import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class Iso_driver {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        ArrayList<String> result = new ArrayList<>();
        Iso_JavaOp javaOp = new Iso_JavaOp();
        Iso_JavsSparkOp javaSparkOp = new Iso_JavsSparkOp();

        long java_start = System.currentTimeMillis();
        javaOp.run();
        long java_end = System.currentTimeMillis();
        result.add("Java运行时间:"+((java_end-java_start)/1000)+"");
        long spark_start = System.currentTimeMillis();
        SparkSession session = SparkSession.builder().master("spark://192.168.35.55:7077").getOrCreate();
        javaSparkOp.run(session);
        session.close();
        long spark_end = System.currentTimeMillis();
        result.add("Spark运行时间:"+((spark_end-spark_start)/1000)+"");
        //python部分


        long end = System.currentTimeMillis();
        result.add("总运行时间:"+((spark_end-spark_start)/1000)+"");
        for(String s:result){
            System.out.println(s);
        }

    }
}
