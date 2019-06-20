package ts.hg_sstreaming.test.utils;

public class pythonUtilsTest {
    public static void main(String[] args) {
        PythonUtils pUtils = new PythonUtils();
        pUtils.execPython("/Users/wangqi/Documents/code/python_wp/f1/python_test.py","");
        while(pUtils.getStatus()!=0){
            System.out.println("running");
           try {
               Thread.sleep(5000);
           }catch (InterruptedException e)
           {
               e.printStackTrace();
           }
        }
    }
}
