package ts.hg_sstreaming.test;

import org.apache.spark.sql.SparkSession;
import ts.hg_sstreaming.test.operator.JavaOp;
import ts.hg_sstreaming.test.operator.SparkOp;
import ts.hg_sstreaming.test.utils.TimeUtils;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Iso_SSparkOp  implements Runnable{
    private final String RUN_ENV =  "product";
    private SparkSession session = null;
    private String path = null;
    private volatile boolean flag  = true;
    private LinkedBlockingQueue<String> inQueue = null;
    private LinkedBlockingQueue<String> outQueue = null;
    private SparkOp spark_op =  null;
    private int file_index = 0;
    private String outputPath = null;
    private List<String> logs = null;

    public void init(){
        spark_op.init();
    }
    public Iso_SSparkOp(SparkSession session, String out, String path, List<String> log){
        inQueue = new LinkedBlockingQueue<>();
        this.session = session;
        this.outputPath = out;
        this.path = path;
        spark_op = new SparkOp(path,session);
        this.logs = log;

    }

    public  LinkedBlockingQueue<String> getInQueue(){
        return inQueue;
    }
    public void setOutQueue( LinkedBlockingQueue<String> outQueue){
        this.outQueue = outQueue;
    }

    public void runConsumer()  throws InterruptedException{
        while(flag){
            String dataPath = inQueue.poll(2, TimeUnit.SECONDS);
            if(dataPath==null){
                Thread.sleep(2000);
                continue;
            }else{
                //run
                long start_time = System.currentTimeMillis();
                logs.add("Spark start:"+ TimeUtils.tranTime(start_time)+"  input:"+dataPath);
                String result = spark_op.run(dataPath,outputPath+file_index+".csv");
                long end_time = System.currentTimeMillis();
                logs.add("Spark end:"+ TimeUtils.tranTime(start_time)+"  cost:"+(end_time-start_time)/1000);
                file_index++;
                if(outQueue!=null){
                    outQueue.offer(result);
                }


            }



        }
    }
    public void runTest()  throws InterruptedException{
        while(flag){
            String dataPath = inQueue.poll(2, TimeUnit.SECONDS);
            if(dataPath==null){
                Thread.sleep(2000);
                continue;
            }else{
                //run

                System.out.println("in SparkJava ,receive data:"+dataPath);
                if(outQueue!=null){
                    outQueue.offer(dataPath);
                }

            }



        }
    }
    @Override
    public void run() {
        switch (RUN_ENV){
            case "test":
                try {
                    runTest();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                break;
            case "product":
                try {
                    runConsumer();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                break;
        }
    }

    public void stop(){
        flag = false;

        while(flag){
            flag = false;
        }
    }
}
