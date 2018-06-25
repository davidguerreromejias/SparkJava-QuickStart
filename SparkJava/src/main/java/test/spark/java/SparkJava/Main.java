package test.spark.java.SparkJava;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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
        
        long numAs = 0;
        long numBs = 0;
        for (String fileName : fileSet) {
            Dataset<String> tempDataset = spark.read().textFile(fileName).cache();
            numAs += tempDataset.filter(s -> s.contains("ESP")).count();
            numBs += tempDataset.filter(s -> s.contains("b")).count(); 
        }
        
        //create a dataset with a defined schema
        datasetWithSchema(spark);
        
        
        //Dataset<String> logData = spark.read().textFile(logFile).cache();
        // DATASET
        // with header
        //Dataset<Row> df = spark.read().option("header", "true").csv("C:\\Users\\Lenovo\\Documents\\fifa-world-cup\\WorldCupMatches.csv");
        //df.show();
        // without header
        Dataset<Row> df = spark.read().csv("C:\\Users\\Lenovo\\Documents\\fifa-world-cup\\WorldCupMatches.csv");
        df.show();
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

	private static void datasetWithSchema(SparkSession spark) {
		
		JavaRDD<String> peopleRDD = spark.sparkContext()
		  .textFile("C:\\Users\\Lenovo\\Documents\\fifa-world-cup\\WorldCupMatches.csv",1)
		  .toJavaRDD();

		// The schema is encoded in a string
		String schemaString = "x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (matches) to Rows
		JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
		  String[] attributes = record.split(",");
		  return RowFactory.create(attributes[0], attributes[1].trim());
		});

		// Apply the schema to the RDD
		Dataset<Row> matchesDataFrame = spark.createDataFrame(rowRDD, schema);

	}
}
