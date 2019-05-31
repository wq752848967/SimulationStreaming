package ts.sstreaming.test;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

public class DataSourceOp {

    SparkSession session  = null;
    public DataSourceOp(SparkSession session){

        this.session = session;
    }
    public Dataset<String> getBatchDs(String path){

        Dataset<Row> ds =   session.read().text(path);
        return ds.as(Encoders.STRING());

    }
    public Dataset<String> getStreamDs(String path){
        return session.readStream().textFile(path);
    }
    public Dataset<Row> getBatchDsRow(String path){

        Dataset<Row> ds =   session.read().option("header","true").csv(path);
        return ds;

    }
    public Dataset<Row> getStreamDsRow(String path, StructType struct){
        return session.readStream().option("header","true").schema(struct).csv(path);
    }
}
