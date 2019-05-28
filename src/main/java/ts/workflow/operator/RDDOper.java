package ts.workflow.operator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import scala.tools.scalap.scalax.rules.InRule;
import ts.workflow.lib.FloKAlgorithm;
import ts.workflow.lib.FloKDataSet;

import java.io.Serializable;
import java.util.HashMap;

public class RDDOper extends FloKAlgorithm implements Serializable {
    @Override
    public FloKDataSet run(FloKDataSet inputDataSets, HashMap<String, String> params) {
        Dataset<Row> ds = inputDataSets.get(0);
        StructType scheme  = ds.schema();
         JavaRDD<Row> jrdd = ds.toJavaRDD();
        JavaRDD filter_rdd = jrdd.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                int id =  Integer.parseInt(row.getAs("id"));
                if(id>10000){
                   return false;
                }
                    return true;

            }
        });
        FloKDataSet flok_ds = new FloKDataSet();
        flok_ds.addDF(super.sparkSession.createDataFrame(filter_rdd,scheme));

        return flok_ds;
    }
}
