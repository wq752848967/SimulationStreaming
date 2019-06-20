package ts.hg_sstreaming.test.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtils {
    public static String tranTime(long ts){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = new Date(ts);
        res = simpleDateFormat.format(date);
        return res;
    }
}
