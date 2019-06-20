package ts.hg_sstreaming.test;

import scala.tools.nsc.Global;
import ts.hg_sstreaming.test.utils.PythonUtils;
import ts.hg_sstreaming.test.utils.TimeUtils;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Iso_SPython implements Runnable {
    private LinkedBlockingQueue<String> inQueue = null;
    private int file_index = 0;
    private String outputPath = null;
    private volatile boolean flag  = true;
    private PythonUtils pythonOp  = null;
    private List<String> logs = null;
    private String python_right_path = null;
    public Iso_SPython(String outputPath, String python_right_path, List<String> logs ) {
        this.outputPath = outputPath;
        inQueue =  new LinkedBlockingQueue<>();
        this.logs = logs;
        this.python_right_path = python_right_path;
    }

    public void init(){
        pythonOp = new PythonUtils();

    }
    @Override
    public void run() {
        try {
            runConsumer();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public void runConsumer()throws InterruptedException{
        while(flag){
            String dataPath = inQueue.poll(2, TimeUnit.SECONDS);
            if(dataPath==null){
                Thread.sleep(2000);
                continue;
            }else{
                //run
                long start_time = System.currentTimeMillis();
                logs.add("Python start:"+ TimeUtils.tranTime(start_time)+"  input:"+dataPath);
                pythonOp.run(dataPath,python_right_path,outputPath+file_index+".csv");
                long end_time = System.currentTimeMillis();
                logs.add("Python end:"+ TimeUtils.tranTime(start_time)+"  cost:"+(end_time-start_time)/1000);

                file_index++;



            }



        }
    }

    public LinkedBlockingQueue<String> getInQueue() {
        return inQueue;
    }

    public void setInQueue(LinkedBlockingQueue<String> inQueue) {
        this.inQueue = inQueue;
    }

    public int getFile_index() {
        return file_index;
    }

    public void setFile_index(int file_index) {
        this.file_index = file_index;
    }
    public void stop(){
        this.flag = false;
    }
}
