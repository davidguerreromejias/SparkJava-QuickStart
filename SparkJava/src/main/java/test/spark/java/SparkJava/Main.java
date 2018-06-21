package test.spark.java.SparkJava;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Spark quick start with java
 *
 */
public class Main 
{
    public static void main( String[] args )
    {
    	String logFile = "C:\\Users\\Lenovo\\Documents\\RandomText.txt";
        SparkSession spark = SparkSession.builder().master("local").appName("Count number of lines with x letter").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
