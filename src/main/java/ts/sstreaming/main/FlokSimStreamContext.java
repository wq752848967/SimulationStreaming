package ts.sstreaming.main;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.SparkSession;
import ts.sstreaming.engine.SparkStreamSchedular;

/**
 *
 * 缺点：
 * 不支持一次性读取配置数据
 * 不支持多数据流join于一点
 * 多线程消费队列问题
 *
 *
 */
public class FlokSimStreamContext {
    //空字符串意味着local[2]模式
    private static String masterUrl = "";
    private static String definition = "";
    private static String jarPath = "";

    /**
     * 算子数目 和 threadNum取最大值
     */
    private static int threadNum = 5;
    public static void main(String[] args) throws Exception{

        CommandLineParser parser = new BasicParser( );
        Options options = new Options( );
        options.addOption("h", "help", false, "Print this usage information");
        options.addOption("m", "master", false, "Print out master information" );
        options.addOption("d", "definition", true, "definition to schedular");
        options.addOption("j", "jarpath", true, "the path of jar");
        options.addOption("t", "thread", true, "the number of thread");
        // Parse the program arguments
        CommandLine commandLine = parser.parse(options,args);
        if(commandLine.hasOption("m")){
            System.out.println(commandLine.getOptionValue("m"));
            masterUrl = commandLine.getOptionValue("m");
        }
        if(commandLine.hasOption("d")){
            System.out.println(commandLine.getOptionValue("d"));;
            definition = commandLine.getOptionValue("d");
        }
        if(commandLine.hasOption("j")){
            System.out.println(commandLine.getOptionValue("j"));;
            jarPath = commandLine.getOptionValue("j");
        }
        if(commandLine.hasOption("t")){
            System.out.println(commandLine.getOptionValue("t"));;
            threadNum = Integer.parseInt(commandLine.getOptionValue("t"));
        }



        SparkSession session = SparkSession.builder().master(masterUrl).getOrCreate();



        SparkStreamSchedular streamShcedular = new SparkStreamSchedular(session,jarPath,definition,5);
        streamShcedular.start();

        System.out.println("main is ok");
        //CommandLine commandLine = parser.parse( options, args );

    }
}
