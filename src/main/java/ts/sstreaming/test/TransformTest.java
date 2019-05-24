package ts.sstreaming.test;

import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TransformTest {
    public static void main(String[] args) {
        List<String>  logs = new ArrayList<>();
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        long start = System.currentTimeMillis();
        Dataset<Row> ds = spark.read().option("header","true").csv("/Users/wangqi/Desktop/FloK/sim/sim_test_small.csv");
        long read = System.currentTimeMillis();
        logs.add("read time cost:"+(read-start)/1000);
        //System.out.println();
        ds.registerTempTable("t1");
        Dataset<Row> ds2 = spark.sql("select max(host) as host,max(id) as id from t1 group by id order by id");
        ds2.registerTempTable("t2");
        Dataset<Row> ds3 = spark.sql("select max(host) as host,max(id) as id from t2 group by id order by id");

        long end = System.currentTimeMillis();
        //System.out.println("sim end time cost:"+(end-start)/1000);
        logs.add("sim end time cost:"+(end-start)/1000);


        //ds3.show();
        RDD<Row> rdd = ds3.rdd();
        StructType sche = ds3.schema();
        ds3.sparkSession().sparkContext().runJob(rdd, new JobFunc(), ClassTag$.MODULE$.apply( Void.class ));
        ds3 = spark.createDataFrame(rdd,sche);
        long run_end = System.currentTimeMillis();
        //System.out.println();
        logs.add("runjob time cost:"+(run_end-start)/1000);


        //next sql
        ds3.registerTempTable("t4");
        Dataset<Row> ds4 = spark.sql("select max(host) as host,max(id) as id from t4 group by id order by id");
        ds4.registerTempTable("t5");
        Dataset<Row> ds5 = spark.sql("select max(host) as host,max(id) as id from t5 group by id order by id");
        System.out.println(ds5.count());
        long show_end = System.currentTimeMillis();
        logs.add("show time cost:"+(show_end-start)/1000);

        //System.out.println();
        spark.close();
        long fin_end = System.currentTimeMillis();
        for(String s:logs){
            System.out.println(s);
        }
        System.out.println("fin end time cost:"+(fin_end-start)/1000);
    }
}
class JobFunc extends AbstractFunction1<Iterator<Row>, Void> implements Serializable {


@Override
public Void apply(Iterator<Row> iterator) {

        return null;
        }
}