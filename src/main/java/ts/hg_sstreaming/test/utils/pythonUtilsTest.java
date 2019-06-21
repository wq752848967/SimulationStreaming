package ts.hg_sstreaming.test.utils;

public class pythonUtilsTest {
    public static void main(String[] args) {
        PythonUtils pUtils = new PythonUtils();
        pUtils.run("hdfs://192.168.35.55:9000/flok/sim/spark_out/0.csv","hdfs://192.168.35.55:9000/flok/4665/csv_loader-1530083012_dafde4f6-9eda-404a-87df-c2fc51bf0dab_0.output","/flok/sim/model/test2.model");
        System.out.println("over");
    }
}
