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
import static org.apache.spark.sql.functions.col;

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
        
        /*long numAs = 0;
        long numBs = 0;
        for (String fileName : fileSet) {
            Dataset<String> tempDataset = spark.read().textFile(fileName).cache();
            numAs += tempDataset.filter(s -> s.contains("ESP")).count();
            numBs += tempDataset.filter(s -> s.contains("b")).count(); 
        }
        
        //Dataset<String> logData = spark.read().textFile(logFile).cache();*/
        // DATASET
        Dataset<Row> df = spark.read().csv("C:\\Users\\Lenovo\\Documents\\fifa-world-cup\\WorldCupMatches.csv");
        //df.show();
        //df.select("_c5", "_c6", "_c7", "_c8").show();
        //df.select("_c3").contains("Spain");
        
        
        //Spark sql
        df.createOrReplaceTempView("matches");
        
        //WINS
        Dataset<Row> sqlDF = spark.sql("SELECT _c5, _c6, _c7, _c8 from matches where _c5 = 'Spain' or _c8 = 'Spain'");
        Dataset<Row> homeMatchesSpain = spark.sql("SELECT * from matches where _c5 = 'Spain'");
        //homeMatchesSpain.show();
        Dataset<Row> awayMatchesSpain = spark.sql("SELECT * from matches where _c8 = 'Spain'");
        sqlDF.createOrReplaceTempView("spainMatches");
        homeMatchesSpain.createOrReplaceTempView("homeSpainMatches");
        awayMatchesSpain.createOrReplaceTempView("awaySpainMatches");
        Dataset<Row> winsHome = spark.sql("SELECT * FROM homeSpainMatches  WHERE _c5 = 'Spain' and _c6 > _c7");
        Dataset<Row> winsAway = spark.sql("SELECT * FROM awaySpainMatches  WHERE _c8 = 'Spain' and _c7 > _c6");
        long totalWins = winsHome.count();
        totalWins += winsAway.count();
        
        //LOSSES
        Dataset<Row> lossesHome = spark.sql("SELECT * FROM homeSpainMatches  WHERE _c5 = 'Spain' and _c6 < _c7");
        Dataset<Row> lossesAway = spark.sql("SELECT * FROM awaySpainMatches  WHERE _c8 = 'Spain' and _c7 < _c6");
        long totalLosses = lossesHome.count();
        totalLosses += lossesAway.count();
        
        //DRAWS
        Dataset<Row> drawsHome = spark.sql("SELECT * FROM homeSpainMatches  WHERE _c5 = 'Spain' and _c6 = _c7");
        Dataset<Row> drawsAway = spark.sql("SELECT * FROM awaySpainMatches  WHERE _c8 = 'Spain' and _c7 = _c6");
        long totalDraws = drawsHome.count();
        totalDraws += drawsAway.count();
        
        System.out.println("Spain has win " + totalWins+ " matches of all that has played in the World Cups.");
        System.out.println("Spain has lose " + totalLosses + " matches of all that has played in the World Cups.");
        System.out.println("Spain has draw " + totalDraws + " matches of all that has played in the World Cups.");
        
        
        //RDD
        /*JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<String> rows = sc.textFile("C:\\Users\\Lenovo\\Documents\\fifa-world-cup\\WorldCupMatches.csv");
        JavaRDD<String> spainMatches = rows.filter(s -> s.contains("Spain"));
        System.out.println(rows.first());

        System.out.println("Lines with ESP: " + numAs + ", lines with b: " + numBs);*/

        spark.stop();
    }
}
