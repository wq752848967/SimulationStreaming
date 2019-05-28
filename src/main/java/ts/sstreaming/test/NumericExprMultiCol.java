package ts.sstreaming.test;


import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataTypes;



import ts.workflow.lib.FloKAlgorithm;
import ts.workflow.lib.FloKDataSet;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Created by mafukun on 2018/7/9.
 */
public class NumericExprMultiCol extends FloKAlgorithm implements java.io.Serializable{
    @Override
    public FloKDataSet run(FloKDataSet inputDataSets, HashMap<String, String> params) {

        String[] dataColNames = params.get("data_col_names").split(",");
        String expression = params.get("expression");
        String outputColName = params.get("output_col_name");
        //double nullReturn = Double.parseDouble(params.getOrDefault("null_return", "0"));
        Dataset<Row> inputDs = inputDataSets.get(0);
        FloKDataSet result = new FloKDataSet();
        inputDs.createOrReplaceTempView("temp");
        Dataset<Row> outputDs = inputDs;
        SQLContext sqlc = super.sparkSession.sqlContext();

        switch(dataColNames.length) {
            case 2:
                sqlc.udf().register("eval_expr",new UDF2<String,String,Double>(){
                    public Double call(String s0, String s1) {
                        if(s0 == null || s1 == null)
                            return null;
                        String str = expression.replaceAll("x0", s0);
                        str = str.replaceAll("x1", s1);
                        double r = 0;
                        try {
                            ScriptEngine jse = new ScriptEngineManager().getEngineByName("JavaScript");
                            r = Double.parseDouble(jse.eval(str).toString());
                        } catch (NumberFormatException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (ScriptException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return r;
                    }},DataTypes.DoubleType);
                outputDs= sqlc.sql("select *,eval_expr(" + dataColNames[0] + "," + dataColNames[1] +") as "+ outputColName + " from temp");
                break;
            case 3:
                sqlc.udf().register("eval_expr",new UDF3<String,String,String,Double>(){
                    public Double call(String s0, String s1, String s2) {
                        if(s0 == null || s1 == null || s2 == null)
                            return null;
                        String str = expression.replaceAll("x0", s0);
                        str = str.replaceAll("x1", s1);
                        str = str.replaceAll("x2", s2);
                        double r = 0;
                        try {
                            ScriptEngine jse = new ScriptEngineManager().getEngineByName("JavaScript");
                            r = Double.parseDouble(jse.eval(str).toString());
                        } catch (NumberFormatException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (ScriptException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return r;
                    }},DataTypes.DoubleType);
                outputDs= sqlc.sql("select *,eval_expr(" + dataColNames[0] + "," + dataColNames[1] + "," + dataColNames[2] + ") as "+ outputColName + " from temp");
                break;
            case 4:
                sqlc.udf().register("eval_expr",new UDF4<String,String,String,String,Double>(){
                    public Double call(String s0, String s1, String s2, String s3) {
                        if(s0 == null || s1 == null || s2 == null || s3 == null)
                            return null;
                        String str = expression.replaceAll("x0", s0);
                        str = str.replaceAll("x1", s1);
                        str = str.replaceAll("x2", s2);
                        str = str.replaceAll("x3", s3);
                        double r = 0;
                        try {
                            ScriptEngine jse = new ScriptEngineManager().getEngineByName("JavaScript");
                            r = Double.parseDouble(jse.eval(str).toString());
                        } catch (NumberFormatException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (ScriptException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return r;
                    }},DataTypes.DoubleType);
                outputDs= sqlc.sql("select *,eval_expr(" + dataColNames[0] + "," + dataColNames[1] + "," + dataColNames[2] + "," + dataColNames[3] +") as "+ outputColName + " from temp");
                break;
            case 5:
                sqlc.udf().register("eval_expr",new UDF5<String,String,String,String,String,Double>(){
                    public Double call(String s0, String s1, String s2, String s3, String s4) {
                        if(s0 == null || s1 == null || s2 == null || s3 == null || s4 == null)
                            return null;
                        String str = expression.replaceAll("x0", s0);
                        str = str.replaceAll("x1", s1);
                        str = str.replaceAll("x2", s2);
                        str = str.replaceAll("x3", s3);
                        str = str.replaceAll("x4", s4);
                        double r = 0;
                        try {
                            ScriptEngine jse = new ScriptEngineManager().getEngineByName("JavaScript");
                            r = Double.parseDouble(jse.eval(str).toString());
                        } catch (NumberFormatException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (ScriptException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return r;
                    }},DataTypes.DoubleType);
                outputDs= sqlc.sql("select *,eval_expr(" + dataColNames[0] + "," + dataColNames[1] + "," + dataColNames[2] + "," + dataColNames[3] + "," + dataColNames[4] + ") as "+ outputColName + " from temp");
                break;
            case 6:
                sqlc.udf().register("eval_expr",new UDF6<String,String,String,String,String,String,Double>(){
                    public Double call(String s0, String s1, String s2, String s3, String s4, String s5) {
                        if(s0 == null || s1 == null || s2 == null || s3 == null || s4 == null || s5 == null)
                            return null;
                        String str = expression.replaceAll("x0", s0);
                        str = str.replaceAll("x1", s1);
                        str = str.replaceAll("x2", s2);
                        str = str.replaceAll("x3", s3);
                        str = str.replaceAll("x4", s4);
                        str = str.replaceAll("x5", s5);
                        double r = 0;
                        try {
                            ScriptEngine jse = new ScriptEngineManager().getEngineByName("JavaScript");
                            r = Double.parseDouble(jse.eval(str).toString());
                        } catch (NumberFormatException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (ScriptException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return r;
                    }},DataTypes.DoubleType);
                outputDs= sqlc.sql("select *,eval_expr(" + dataColNames[0] + "," + dataColNames[1] + "," + dataColNames[2] + "," + dataColNames[3] + "," + dataColNames[4] + "," + dataColNames[5] +") as "+ outputColName + " from temp");
                break;
            case 7:
                sqlc.udf().register("eval_expr",new UDF7<String,String,String,String,String,String,String,Double>(){
                    public Double call(String s0, String s1, String s2, String s3, String s4, String s5, String s6) {
                        if(s0 == null || s1 == null || s2 == null || s3 == null || s4 == null || s5 == null || s6 == null)
                            return null;
                        String str = expression.replaceAll("x0", s0);
                        str = str.replaceAll("x1", s1);
                        str = str.replaceAll("x2", s2);
                        str = str.replaceAll("x3", s3);
                        str = str.replaceAll("x4", s4);
                        str = str.replaceAll("x5", s5);
                        str = str.replaceAll("x6", s6);
                        double r = 0;
                        try {
                            ScriptEngine jse = new ScriptEngineManager().getEngineByName("JavaScript");
                            r = Double.parseDouble(jse.eval(str).toString());
                        } catch (NumberFormatException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (ScriptException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return r;
                    }},DataTypes.DoubleType);
                outputDs= sqlc.sql("select *,eval_expr(" + dataColNames[0] + "," + dataColNames[1] + "," + dataColNames[2] + "," + dataColNames[3] + "," + dataColNames[4] + "," + dataColNames[5] + "," + dataColNames[6] +") as "+ outputColName + " from temp");
                break;
            case 8:
                sqlc.udf().register("eval_expr",new UDF8<String,String,String,String,String,String,String,String,Double>(){
                    public Double call(String s0, String s1, String s2, String s3, String s4, String s5, String s6, String s7) {
                        if(s0 == null || s1 == null || s2 == null || s3 == null || s4 == null || s5 == null || s6 == null || s7 == null)
                            return null;
                        String str = expression.replaceAll("x0", s0);
                        str = str.replaceAll("x1", s1);
                        str = str.replaceAll("x2", s2);
                        str = str.replaceAll("x3", s3);
                        str = str.replaceAll("x4", s4);
                        str = str.replaceAll("x5", s5);
                        str = str.replaceAll("x6", s6);
                        str = str.replaceAll("x7", s7);
                        double r = 0;
                        try {
                            ScriptEngine jse = new ScriptEngineManager().getEngineByName("JavaScript");
                            r = Double.parseDouble(jse.eval(str).toString());
                        } catch (NumberFormatException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (ScriptException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return r;
                    }},DataTypes.DoubleType);
                outputDs= sqlc.sql("select *,eval_expr(" + dataColNames[0] + "," + dataColNames[1] + "," + dataColNames[2] + "," + dataColNames[3] + "," + dataColNames[4] + "," + dataColNames[5]+ "," + dataColNames[6] + "," + dataColNames[7] +") as "+ outputColName + " from temp");
                break;
            case 9:
                sqlc.udf().register("eval_expr",new UDF9<String,String,String,String,String,String,String,String,String,Double>(){
                    public Double call(String s0, String s1, String s2, String s3, String s4, String s5, String s6, String s7, String s8) {
                        if(s0 == null || s1 == null || s2 == null || s3 == null || s4 == null || s5 == null || s6 == null || s7 == null || s8 == null)
                            return null;
                        String str = expression.replaceAll("x0", s0);
                        str = str.replaceAll("x1", s1);
                        str = str.replaceAll("x2", s2);
                        str = str.replaceAll("x3", s3);
                        str = str.replaceAll("x4", s4);
                        str = str.replaceAll("x5", s5);
                        str = str.replaceAll("x6", s6);
                        str = str.replaceAll("x7", s7);
                        str = str.replaceAll("x8", s8);
                        double r = 0;
                        try {
                            ScriptEngine jse = new ScriptEngineManager().getEngineByName("JavaScript");
                            r = Double.parseDouble(jse.eval(str).toString());
                        } catch (NumberFormatException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (ScriptException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return r;
                    }},DataTypes.DoubleType);
                outputDs= sqlc.sql("select *,eval_expr(" + dataColNames[0] + "," + dataColNames[1] + "," + dataColNames[2] + "," + dataColNames[3] + "," + dataColNames[4] + "," + dataColNames[5]+ "," + dataColNames[6] + "," + dataColNames[7] + "," + dataColNames[8] +") as "+ outputColName + " from temp");
                break;
            case 10:
                sqlc.udf().register("eval_expr",new UDF10<String,String,String,String,String,String,String,String,String,String,Double>(){
                    public Double call(String s0, String s1, String s2, String s3, String s4, String s5, String s6, String s7, String s8, String s9) {
                        if(s0 == null || s1 == null || s2 == null || s3 == null || s4 == null || s5 == null || s6 == null || s7 == null || s8 == null || s9 == null)
                            return null;
                        String str = expression.replaceAll("x0", s0);
                        str = str.replaceAll("x1", s1);
                        str = str.replaceAll("x2", s2);
                        str = str.replaceAll("x3", s3);
                        str = str.replaceAll("x4", s4);
                        str = str.replaceAll("x5", s5);
                        str = str.replaceAll("x6", s6);
                        str = str.replaceAll("x7", s7);
                        str = str.replaceAll("x8", s8);
                        str = str.replaceAll("x9", s9);
                        double r = 0;
                        try {
                            ScriptEngine jse = new ScriptEngineManager().getEngineByName("JavaScript");
                            r = Double.parseDouble(jse.eval(str).toString());
                        } catch (NumberFormatException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (ScriptException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return r;
                    }},DataTypes.DoubleType);
                outputDs= sqlc.sql("select *,eval_expr(" + dataColNames[0] + "," + dataColNames[1] + "," + dataColNames[2] + "," + dataColNames[3] + "," + dataColNames[4] + "," + dataColNames[5]+ "," + dataColNames[6] + "," + dataColNames[7] + "," + dataColNames[8] + "," + dataColNames[9] +") as "+ outputColName + " from temp");
                break;
            default:

        }

        result.addDF(outputDs);
        return result;
    }
}