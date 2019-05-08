package ts.sstreaming.utils.ut;


import junit.framework.TestCase;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ts.workflow.lib.FloKAlgorithm;
import ts.workflow.lib.FloKAlgorithmArguments;
import ts.workflow.lib.FloKDataSet;
import ts.sstreaming.utils.impl.JarObjectLoaderImpl;
import ts.sstreaming.utils.inter.ObjectLoaderInter;

import java.util.HashMap;

public class JarObjectLoaderUT extends TestCase {
    private String jarPath = "file:///Users/wangqi/Downloads/combiner-oper-0.1.0-SNAPSHOT-jar-with-dependencies.jar";
    private String className = "ts.workflow.operator.HDFSReader";
    ObjectLoaderInter testObj = null;
    @Before
    protected void setUp() throws Exception {


        testObj = new JarObjectLoaderImpl();

    }

    @After
    protected void tearDown() throws Exception {

    }
    @Test
    public void testRun(){
        FloKAlgorithm alg = (FloKAlgorithm)testObj.loadObject(jarPath,className);
        FloKAlgorithmArguments argum = new FloKAlgorithmArguments();
        argum.mainclass = className;
        alg.initAlgorithm(argum);
        FloKDataSet input = new FloKDataSet();
        HashMap<String,String> map = new HashMap<>();
        map.put("header_text","");
        map.put("delimiter",",");
        map.put("path","/Users/wangqi/Desktop/Flok/data/data.csv");
        alg.run(input,map).get(0).show();
//        SparkSession session =  SparkSession.builder().master("local[2]").getOrCreate();
//        session.read().csv("/Users/wangqi/Desktop/Flok/data/data.csv").show();

    }
}
