package ts.sstreaming.main;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.SparkSession;
import ts.sstreaming.engine.SparkStreamSchedular;

public class FlokSimStreamContext {
    private static String masterUrl = "";
    private static String definition = "";
    private static String jarPath = "";
    public static void main(String[] args) throws Exception{

        CommandLineParser parser = new BasicParser( );
        Options options = new Options( );
        options.addOption("h", "help", false, "Print this usage information");
        options.addOption("m", "master", false, "Print out master information" );
        options.addOption("d", "definition", true, "definition to schedular");
        options.addOption("j", "jarpath", true, "the path of jar");
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



        SparkSession session = SparkSession.builder().master(masterUrl).getOrCreate();



        SparkStreamSchedular streamShcedular = new SparkStreamSchedular(session,jarPath,definition);
        streamShcedular.start();

        //CommandLine commandLine = parser.parse( options, args );

    }
}
