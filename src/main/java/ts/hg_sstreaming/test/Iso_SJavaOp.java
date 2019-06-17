package ts.hg_sstreaming.test;

import org.apache.spark.sql.SparkSession;
import ts.hg_sstreaming.test.operator.JavaOp;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Iso_SJavaOp implements Runnable{

    //获取实例方法
    //监控实例
    //队列
    //下一个队列

    private SparkSession session = null;
    private int maxInstance = 0;
    private volatile int curInstance = 0;
    private String path = null;
    private volatile boolean flag  = true;
    private LinkedBlockingQueue<String> inQueue = null;
    private LinkedBlockingQueue<String> outQueue = null;
    private JavaOp java_op = null;

    private String outputPath = null;

    public Iso_SJavaOp(SparkSession session,String out,String path){
        inQueue = new LinkedBlockingQueue<>();
        this.session = session;
        this.outputPath = out;
        this.path = path;
        java_op = new JavaOp(path,session);


    }
    public void init(){
        java_op.init();
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
                outQueue.offer(java_op.run(dataPath,""));

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
