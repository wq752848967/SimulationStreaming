package ts.sstreaming.test;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class TransformTest2 {
//    public static void main(String[] args) {
//
//        SparkSession spark = SparkSession
//                .builder()
//                .master("local[2]")
//                .appName("JavaStructuredNetworkWordCount")
//                .getOrCreate();
//        long start = System.currentTimeMillis();
//        Dataset<Row> ds = spark.read().option("header","true").csv("/Users/wangqi/Desktop/FloK/sim/sim_test_small.csv");
//        long read = System.currentTimeMillis();
//        System.out.println("read time cost:"+(read-start)/1000);
//        ds.registerTempTable("t1");
//        Dataset<Row> ds2 = spark.sql("select max(host) as host,max(id) as id from t1 group by id order by id");
//        //spark.sparkContext().run
//        ds2.registerTempTable("t3");
//        Dataset<Row> ds3 = spark.sql("select max(host) as host,max(id) as id from t3 group by id order by id");
//        long end = System.currentTimeMillis();
//        System.out.println("sim end time cost:"+(end-start)/1000);
//
//
//
//        //ds3.show();
//        //RDD<Row> rdd = ds3.rdd();
//        //SparkContext ctx = spark.sparkContext();
//        ds3.sparkSession().sparkContext().runJob(ds3.rdd(), new JobFunc(), ClassTag$.MODULE$.apply( Void.class ));
//
//        //long run_end = System.currentTimeMillis();
//        //System.out.println("runjob time cost:"+(run_end-start)/1000);
//
//        //Row[] rows =  (Row[])rdd.collect();
//        //System.out.println(rows.length);
//
//        ds3.registerTempTable("t4");
//        Dataset<Row> ds4 = spark.sql("select max(host) as host,max(id) as id from t4 group by id order by id");
//        //spark.sparkContext().run
//        ds4.registerTempTable("t5");
//        Dataset<Row> ds5 = spark.sql("select max(host) as host,max(id) as id from t5 group by id order by id");
////        System.out.println(ds5.count());
//        RDD<Row> rdd = ds5.rdd();
//        ds5.sparkSession().sparkContext().runJob(rdd, new JobFunc(), ClassTag$.MODULE$.apply( Void.class ));
//        long show_end = System.currentTimeMillis();
//        System.out.println("show time cost:"+(show_end-start)/1000);
//        System.out.println(rdd.count());
//        spark.close();
//        long fin_end = System.currentTimeMillis();
//        System.out.println("fin end time cost:"+(fin_end-start)/1000);

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://192.168.10.12:7077")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();

        DataSourceOp dsource = new DataSourceOp(spark);
        boolean batch = true;
        if(args[0].trim()=="1"){
            batch = false;
        }

        Dataset<Row> ds_input = null;
        StructType scheme = new StructType().add("id",IntegerType)
                .add("align_time",StringType)
                .add("host",StringType)
                .add("J_0001_00_247",DoubleType);
        if(batch){
            ds_input = dsource.getBatchDsRow("hdfs://192.168.10.12:9000/sim_big_36.csv");

        }else{
            ds_input = dsource.getStreamDsRow("hdfs://192.168.10.12:9000/sim_big_36.csv",scheme);
        }
        //ds_input.show();
        ds_input = ds_input.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row row) throws Exception {
                int id = Integer.parseInt(row.getAs("id").toString());
                if(id%100==0){
                   return false;
                }
                return true;
            }
        });

        Dataset<Tuple2<String,Double>> pair_ds =  ds_input.map(new MapFunction<Row, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {
                int id = Integer.parseInt(row.getAs("id").toString());
                String key = (id%2)==0?"1":"0";
                return new Tuple2<>(key,Double.parseDouble(" "+row.getAs("J_0001_00_247")));
            }
        },Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()));

        Dataset<Row> result = pair_ds.toDF("id","value").groupBy("id").sum("value").withColumnRenamed("sum(value)", "sum");;

//        Dataset<String> ds_sum = ds_input.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
//
//        Dataset<Row> result = ds_sum.groupBy("value").count();

        if(batch){
            result.show();
        }
        else{
            StreamingQuery query = result.writeStream()
                    .outputMode("complete")
                    .format("console")
                    .start();

            query.awaitTermination();
        }
    }
}
