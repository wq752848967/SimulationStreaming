package ts.sstreaming.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        String masterUrl = "local[8]";
        AtomicInteger val  = new AtomicInteger();
        SparkSession session = SparkSession.builder().master(masterUrl).getOrCreate();

        Dataset<Row> ds1 = session.read().option("header","true").csv("hdfs://192.168.10.12:9000/flok/sim_data_small.csv");
        Dataset<Row> ds2 = ds1.coalesce(1);
        ds2.write().option("header","true").csv("/Users/wangqi/Desktop/FloK/sim/sim_test_small.csv");
        //
// Dataset<Row> ds2 = session.read().option("header","true").csv("hdfs://192.168.10.12:9000/flok/layer1_35all_J247.csv");
//        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
//        ExecutorService pool = Executors.newFixedThreadPool(2);
//        pool.submit(new ParTest(ds1,1,queue,val));
//        //.submit(new ParTest(ds2,2,queue,val));
//
//        while(val.get()<1){
//            Thread.sleep(4);
//        }
//        for (int i = 0; i < 2; i++) {
//            System.out.println(queue.poll());
//        }
//        ds.registerTempTable("t");
//        Dataset<Row> new_ds = session.sql("select max(id) from t group by id");
//        new_ds.count();
       // new_ds.write().option("header","true").mode(SaveMode.Overwrite).csv("hdfs://192.168.10.12:9000/flok/sim_data_small.csv");
    }



}
class ParTest implements Runnable{
    private Dataset<Row>  ds =  null;
    private int index;
    AtomicInteger val = null;
    private LinkedBlockingQueue<String> queue = null;
    public ParTest(Dataset<Row> ds,int index,LinkedBlockingQueue<String> queue,AtomicInteger val){
        this.ds = ds;
        this.index = index;
        this.queue = queue;
        this.val = val;
    }
    @Override
    public void run() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        String cur_time = df.format(new Date());
        queue.offer("runId:"+index+"  start_time:"+cur_time);
        ds.count();
        ds.show();
        String end_time = df.format(new Date());
        queue.offer("runId:"+index+"  end_time:"+end_time);
        val.incrementAndGet();
    }
}
