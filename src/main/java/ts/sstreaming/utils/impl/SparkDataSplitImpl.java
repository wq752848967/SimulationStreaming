package ts.sstreaming.utils.impl;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import ts.sstreaming.utils.inter.DataSplitInter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.spark.sql.types.DataTypes.LongType;

public class SparkDataSplitImpl implements DataSplitInter, Serializable {

    private SparkSession sparkSession = null;

    public SparkDataSplitImpl(SparkSession session) {
        this.sparkSession = session;
    }

    @Override
    public Dataset<Row>[] splitDataByRecordCount(Dataset<Row> ds, int count) {
        long row_count = ds.count();
        int data_split_loop_count = (int)row_count/count;
        int last_ds = row_count%count==0?0:1;
        Dataset<Row>[] result = new Dataset[data_split_loop_count+last_ds];
        ds.schema();
        ds.printSchema();
        //添加id_number列
        StructType ds_scheme = ds.schema().add("id_number",LongType);
        JavaRDD<Row> rdd_ds = ds.toJavaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> rowLongTuple2) throws Exception {
                List<Object> list = new ArrayList<>();
                List type_list = scala.collection.JavaConversions.seqAsJavaList(rowLongTuple2._1.toSeq());
                for(int i=0;i<type_list.size();i++){
                    list.add(type_list.get(i));
                }
                list.add(RowFactory.create(rowLongTuple2._2).toSeq().last());

                return new GenericRowWithSchema(list.toArray(),ds_scheme);

            }
        });
        Dataset<Row> result_ds = sparkSession.createDataFrame(rdd_ds,ds_scheme);
        result_ds.registerTempTable("split_table");

        int startIndex = 0;
        int addLoop = 0;
        for (int i = 0; i <=data_split_loop_count; i++) {
            result[i] = (sparkSession.sql("select * from split_table where id_number>="
                    +(startIndex+addLoop*count)
                    +" and id_number<"+(startIndex+(addLoop+1)*count)
                    ).drop("id_number")
            );
            addLoop++;
        }
        return result;
    }

    @Override
    public Dataset<Row>[] splitDataByBatch(Dataset<Row> ds, int batchCount) {

        double[] rating = new double[batchCount];
        Arrays.fill(rating,1.0/batchCount);
        Dataset<Row>[] result = ds.randomSplit(rating);
        return result;

    }

    @Override
    public Dataset<Row>[] splitDataByTimeWindows(Dataset<Row> ds,String time_col_name, Long windows_interval) {
        List<Dataset<Row>> reuslt_list = new ArrayList<>();
        String[] cols_arr = ds.columns();
        boolean flag = false;
        for(String item:cols_arr){
            if(item.equals(time_col_name)){
                flag = true;
            }
        }
        if(!flag){
            return null;
        }


        //main alg
        ds.registerTempTable("time_data");
        long start_time = Long.parseLong(sparkSession.sql("select min("+time_col_name+") from time_data").first().get(0)+"");
        long end_time = Long.parseLong(sparkSession.sql("select max("+time_col_name+") from time_data").first().get(0)+"");
        long index_time = start_time;
        while(index_time!=end_time){
            Dataset<Row> temp_ds = sparkSession.sql("select * from time_data where "
                    +time_col_name+">="+index_time
                    +" and "+time_col_name+"<="+(index_time+windows_interval)+"");
            if(temp_ds.count()!=0){
                reuslt_list.add(temp_ds);
            }
            index_time+=windows_interval;
        }
        Dataset<Row>[] result = new Dataset[reuslt_list.size()];
        return reuslt_list.toArray(result);
    }
}
