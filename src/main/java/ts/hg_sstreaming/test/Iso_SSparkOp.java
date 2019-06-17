package ts.hg_sstreaming.test;

import org.apache.spark.sql.SparkSession;
import ts.hg_sstreaming.test.operator.JavaOp;
import ts.hg_sstreaming.test.operator.SparkOp;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Iso_SSparkOp  implements Runnable{
    private SparkSession session = null;
    private String path = null;
    private volatile boolean flag  = true;
    private LinkedBlockingQueue<String> inQueue = null;
    private LinkedBlockingQueue<String> outQueue = null;
    private SparkOp spark_op =  null;

    private String outputPath = null;


    public void init(){
        spark_op.init();
    }
    public Iso_SSparkOp(SparkSession session,String out,String path){
        inQueue = new LinkedBlockingQueue<>();
        this.session = session;
        this.outputPath = out;
        this.path = path;
        spark_op = new SparkOp(path,session);

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

                outQueue.offer(spark_op.run(dataPath,""));

            }



        }
    }

    @Override
    public void run() {
        try {
            runConsumer();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public void stop(){
        flag = false;

        while(flag){
            flag = false;
        }
    }
}
