package ts.hg_sstreaming.test;

import org.apache.spark.sql.SparkSession;
import ts.hg_sstreaming.test.operator.JavaOp;
import ts.hg_sstreaming.test.utils.TimeUtils;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Iso_SJavaOp implements Runnable{

    //获取实例方法
    //监控实例
    //队列
    //下一个队列
    private final String RUN_ENV =  "product";
    private SparkSession session = null;
    private int maxInstance = 0;
    private volatile int curInstance = 0;
    private String path = null;
    private volatile boolean flag  = true;
    private LinkedBlockingQueue<String> inQueue = null;
    private LinkedBlockingQueue<String> outQueue = null;
    private JavaOp java_op = null;
    private int file_index =  0;
    private List<String> logs = null;
    private String outputPath = null;

    public Iso_SJavaOp(SparkSession session,String out,String path,List<String> logs){
        inQueue = new LinkedBlockingQueue<>();
        this.session = session;
        this.outputPath = out;
        this.path = path;
        java_op = new JavaOp(path,session);
        this.logs = logs;

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
                long start_time = System.currentTimeMillis();
                logs.add("JAVA start:"+ TimeUtils.tranTime(start_time)+"  input:"+dataPath);
                outQueue.offer(java_op.run(dataPath,outputPath+file_index+".csv"));
                long end_time = System.currentTimeMillis();
                logs.add("JAVA end:"+ TimeUtils.tranTime(start_time)+"  cost:"+(end_time-start_time)/1000);
                file_index++;
            }



        }
    }
    public void runTest()throws InterruptedException{
        while(flag){
            String dataPath = inQueue.poll(2, TimeUnit.SECONDS);
            if(dataPath==null){
                Thread.sleep(2000);
                continue;
            }else{
                System.out.println("in SJava ,receive data:"+dataPath);
                outQueue.offer(dataPath);

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
