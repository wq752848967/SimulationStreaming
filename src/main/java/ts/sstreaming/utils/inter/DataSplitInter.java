package ts.sstreaming.utils.inter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataSplitInter {

    /**
     *
     * @param ds
     * @param count
     * @return
     *
     * 每批次数据划分固定count记录数目
     */
      Dataset<Row>[] splitDataByRecordCount(Dataset<Row> ds,int count);

    /**
     *
     * @param ds
     * @param batchCount
     * @return
     *
     * 划分固定批次的数据组，批次数目=batchCount
     * 注意：
     * 1.划分批次内的记录数量不绝对相等,
     *
     */

      Dataset<Row>[] splitDataByBatch(Dataset<Row> ds,int batchCount);


    /**
     *
     * @param ds
     * @param windows_interval    毫秒
     * @return
     *
     * 按照时间窗口进行数据划分，time_interval为窗口大小
     * 注意：
     * 1.输入时间需要是时间戳格式
     * 2.适用于时间密集型数据，过于稀疏的时间数据排列会导致过多的任务被执行
     *
     *
     */
      Dataset<Row>[] splitDataByTimeWindows(Dataset<Row> ds,String time_col_name,Long windows_interval);


}
