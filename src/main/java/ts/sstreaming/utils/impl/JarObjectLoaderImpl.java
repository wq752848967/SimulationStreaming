package ts.sstreaming.utils.impl;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ts.workflow.lib.FloKAlgorithm;
import ts.sstreaming.utils.inter.ObjectLoaderInter;
import ts.workflow.lib.FloKAlgorithmArguments;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class JarObjectLoaderImpl implements ObjectLoaderInter {
    private static Logger LOGGER = LoggerFactory.getLogger(JarObjectLoaderImpl.class);
    private static int instance_id = 0;
    public FloKAlgorithm loadObject(String path, String className, Object context){
        SparkSession sparkSession = (SparkSession)context;
        FloKAlgorithm alg = (FloKAlgorithm)load(path,className);
        alg.sparkSession = sparkSession;
        return alg;
    }
    public FloKAlgorithm load(String path, String className){
        String filePath = path;
        FloKAlgorithm alg = null;
        URL url;
        try {
            url = new URL(filePath);
        } catch (MalformedURLException e1) {
            e1.printStackTrace();
            LOGGER.info("文件不存在");
            return null;
        }
        URLClassLoader loader = new URLClassLoader(new URL[]{url},Thread.currentThread().getContextClassLoader());
        //URLClassLoader loader = new URLClassLoader(new URL[] { url });
        try {
            Class<?> processorClass = loader.loadClass(className);
            alg = (FloKAlgorithm) processorClass.newInstance();

        } catch (Exception e) {
            LOGGER.info("创建业务类失败");
            e.printStackTrace();
            return null;
        }
        return alg;
    }

}
