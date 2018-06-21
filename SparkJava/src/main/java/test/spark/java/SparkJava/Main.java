package test.spark.java.SparkJava;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark quick start with java
 *
 */
public class Main 
{
    public static void main( String[] args ) throws IOException
    {
    	String logFile = "C:\\Users\\Lenovo\\Documents\\RandomText.txt";
        SparkSession spark = SparkSession.builder().master("local").appName("Count number of lines with x letter").getOrCreate();
        
        Set<String> fileSet = Files.list(Paths.get("C:\\Users\\Lenovo\\Documents\\fifa-world-cup"))
                .filter(name -> name.toString().endsWith(".csv"))
                .map(name -> name.toString())
                .collect(Collectors.toSet());
        
        long numAs = 0;
        long numBs = 0;
        for (String fileName : fileSet) {
            Dataset<String> tempDataset = spark.read().textFile(fileName).cache();
            numAs += tempDataset.filter(s -> s.contains("ESP")).count();
            numBs += tempDataset.filter(s -> s.contains("b")).count(); 
        }
        
        //Dataset<String> logData = spark.read().textFile(logFile).cache();
        
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<String> rows = sc.textFile("C:\\Users\\Lenovo\\Documents\\fifa-world-cup\\WorldCupMatches.csv");
        JavaRDD<String> spainMatches = rows.filter(s -> s.contains("Spain"));
        System.out.println(rows.first());

        System.out.println("Lines with ESP: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
