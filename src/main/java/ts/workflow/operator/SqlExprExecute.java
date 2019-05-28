package ts.workflow.operator;

import ts.workflow.lib.FloKAlgorithm;
import ts.workflow.lib.FloKDataSet;

import java.util.HashMap;

public class SqlExprExecute extends FloKAlgorithm {
    /**
     * Use `super.sparkSession` to get sparksession.
     * Created by yangyawen on 2018/7/18.
     */
    @Override
    public FloKDataSet run(FloKDataSet floKDataSet, HashMap<String, String> params) {

        String sqlExpr = params.get("sql_expr");
        String tableName = params.get("table_name");
        FloKDataSet result = new FloKDataSet();
        floKDataSet.get(0).createOrReplaceTempView(tableName);
        result.addDF(super.sparkSession.sql(sqlExpr));
        return result;
    }
}
