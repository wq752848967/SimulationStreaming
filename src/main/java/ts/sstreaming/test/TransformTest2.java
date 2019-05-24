package ts.sstreaming.test;

import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.List;

public class TransformTest2 {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        long start = System.currentTimeMillis();
        Dataset<Row> ds = spark.read().option("header","true").csv("/Users/wangqi/Desktop/FloK/sim/sim_test_small.csv");
        long read = System.currentTimeMillis();
        System.out.println("read time cost:"+(read-start)/1000);
        ds.registerTempTable("t1");
        Dataset<Row> ds2 = spark.sql("select max(host) as host,max(id) as id from t1 group by id order by id");
        //spark.sparkContext().run
        ds2.registerTempTable("t3");
        Dataset<Row> ds3 = spark.sql("select max(host) as host,max(id) as id from t3 group by id order by id");
        long end = System.currentTimeMillis();
        System.out.println("sim end time cost:"+(end-start)/1000);



        //ds3.show();
        //RDD<Row> rdd = ds3.rdd();
        //SparkContext ctx = spark.sparkContext();
        //ds3.sparkSession().sparkContext().runJob(ds3.rdd(), new JobFunc(), ClassTag$.MODULE$.apply( Void.class ));

        //long run_end = System.currentTimeMillis();
        //System.out.println("runjob time cost:"+(run_end-start)/1000);

        //Row[] rows =  (Row[])rdd.collect();
        //System.out.println(rows.length);

        ds3.registerTempTable("t4");
        Dataset<Row> ds4 = spark.sql("select max(host) as host,max(id) as id from t4 group by id order by id");
        //spark.sparkContext().run
        ds4.registerTempTable("t5");
        Dataset<Row> ds5 = spark.sql("select max(host) as host,max(id) as id from t5 group by id order by id");
//        System.out.println(ds5.count());
        RDD<Row> rdd = ds5.rdd();
        ds5.sparkSession().sparkContext().runJob(rdd, new JobFunc(), ClassTag$.MODULE$.apply( Void.class ));
        long show_end = System.currentTimeMillis();
        System.out.println("show time cost:"+(show_end-start)/1000);
        System.out.println(rdd.count());
        spark.close();
        long fin_end = System.currentTimeMillis();
        System.out.println("fin end time cost:"+(fin_end-start)/1000);
    }
}
