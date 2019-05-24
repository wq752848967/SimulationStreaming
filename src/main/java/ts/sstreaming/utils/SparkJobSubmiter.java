package ts.sstreaming.utils;

import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;


import java.io.Serializable;

public class SparkJobSubmiter {
    public static Dataset<Row> runJob(Dataset<Row> ds){
        RDD<Row> rdd = ds.rdd();
        StructType sche = ds.schema();
        ds.sparkSession().sparkContext().runJob(rdd, new JobFunc(), ClassTag$.MODULE$.apply( Void.class ));
        return ds.sparkSession().createDataFrame(rdd,sche);
    }
}
class JobFunc extends AbstractFunction1<Iterator<Row>, Void> implements Serializable {


    @Override
    public Void apply(Iterator<Row> iterator) {

        return null;
    }
}