package ts.hg_sstreaming.test.utils;

import java.io.IOException;

public class PythonUtils {
    private int status  = 0;
    private String PYTHON_OP = "/Users/wangqi/Documents/code/python_wp/f1/python_test.py";
    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public void run(String input,String right,String out){
        execPython(input,right,out);
    }

    public void execPython(String input,String right,String out){
        Process proc = null;
        status = 1;

       try {
           proc = Runtime.getRuntime().exec("python " + PYTHON_OP+" "+input+" "+right+" "+out);
           //proc.waitFor();
           System.out.println("afer running:"+proc.waitFor()+out);
       }catch (InterruptedException e){
           e.printStackTrace();
       }catch (IOException io){
           io.printStackTrace();
       }finally {
           status = 0;
       }

    }
}

