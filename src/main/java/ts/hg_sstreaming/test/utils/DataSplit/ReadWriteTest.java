package ts.hg_sstreaming.test.utils.DataSplit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

public class ReadWriteTest {
    public static void main(String[] args) {
        SparkSession session_java = SparkSession.builder().master("spark://172.16.244.8:7077").getOrCreate();
        String inpath = args[0];
        String outpath = args[1];
        long start = System.currentTimeMillis();
        //read
        Dataset <Row> ds = session_java.read().option("header","true").option("delimiter",",").csv(inpath);

        ds.write().mode(SaveMode.Overwrite).option("header","true").option("delimiter",",").csv(outpath);

        //write
        long end = System.currentTimeMillis();
        System.out.println("total time:"+(end-start)/100);
    }
}
