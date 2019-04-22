package ts.sstreaming.utils.ut;


import junit.framework.TestCase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ts.sstreaming.utils.impl.SparkDataSplitImpl;

import java.util.Arrays;

public class SparkDataSplitUT extends TestCase {
    SparkSession sparkSession = null;
    Dataset<Row> input = null;
    SparkDataSplitImpl testObj = null;
    @Before
    protected void setUp() throws Exception {
        // 1. 创建sparkSession
        sparkSession = SparkSession.builder().master("local").appName("SparkDataSplitUT").getOrCreate();
        // 对于Windows系统, 需要设置 /hadoop/home/dir


        // 2. 初始化测试数据集, 读取数据并初始化flokDataSet, 作为测试算法的输入
       input = sparkSession.read().option("header","true").option("delimiter",";").csv("/Users/wangqi/Desktop/FloK/data/data.csv");


        // 3. 初始化算法
        testObj = new SparkDataSplitImpl(sparkSession);

    }

    @After
    protected void tearDown() throws Exception {
        sparkSession.stop();
    }
    @Test
    public void testRun(){
        Dataset<Row>[] result = testObj.splitDataByCount(input,3);
        for ( Dataset<Row> item :result) {
            item.show();
        }
    }
}
