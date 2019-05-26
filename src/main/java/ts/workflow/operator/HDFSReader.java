package ts.workflow.operator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ts.workflow.lib.FloKAlgorithm;
import ts.workflow.lib.FloKDataSet;

import java.util.HashMap;

/**
 * Created by wangqi on 2018/9/6.
 */
public class HDFSReader extends FloKAlgorithm {
    @Override
    public FloKDataSet run(FloKDataSet inputDataSets, HashMap<String, String> params) {
        boolean header_flag = false;
        String header = params.get("header_text").trim();
        if(header.length()==0){
            //读取时候不带列名
            header_flag = true;
        }
        Dataset<Row> input = super.sparkSession.read().option("header",header_flag).
                option("delimiter",params.get("delimiter")).csv(params.get("path"));
        if(!header_flag){
            int index =  0;
            String[] headers = header.split(",");
            if(input.columns().length==headers.length){
                for (int i = 0; i < input.columns().length; i++) {
                    input  = input.withColumnRenamed("_c"+index,headers[i]);
                    index++;
                }
            }


        }
        FloKDataSet result = new FloKDataSet();


        input.printSchema();
        result.addDF(input);
        return result;

    }
}
