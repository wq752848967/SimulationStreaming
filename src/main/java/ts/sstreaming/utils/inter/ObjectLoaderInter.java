package ts.sstreaming.utils.inter;

import org.apache.spark.sql.SparkSession;

import java.util.Map;

public interface ObjectLoaderInter{
    Object loadObject(String path, String className, Object context);
}
