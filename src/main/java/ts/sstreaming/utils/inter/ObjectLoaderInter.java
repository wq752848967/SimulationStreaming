package ts.sstreaming.utils.inter;

import java.util.Map;

public interface ObjectLoaderInter{
    Object loadObject(String path, Map<String,Object> param);
}
