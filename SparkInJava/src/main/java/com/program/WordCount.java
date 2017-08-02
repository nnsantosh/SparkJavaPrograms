package com.program;

//import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import static org.apache.spark.sql.functions.col;


public class WordCount {
	
	public static void main(String[] args) {
		
		
		/*SparkConf conf = new SparkConf().setAppName("Line_Count").setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(conf);*/
		/*JavaRDD<String> linesRDD = ctx.textFile("C:\\Users\\SantSavi\\Documents\\Git_Commands.txt");
		System.out.println("WordCount.main() linesRDD count: "+linesRDD.count());*/
		//List<String> jsonData = Arrays.asList(
		        //"{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
		
		String jsonDataStr = 
		        "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}";
	
		
		 /*SparkSession spark = SparkSession.builder().appName("Build a DataFrame from Scratch").master("local[2]")
		            .getOrCreate();*/
		 SparkSession spark = SparkSession
			      .builder().master("local")
			      .appName("Java Spark SQL data sources example")
			      .config("spark.some.config.option", "some-value")
			      .getOrCreate();
		// A JSON dataset is pointed to by path.
		// The path can be either a single text file or a directory storing text files.
		Dataset<Row> outDs = spark.read().json("data/test.json");
		outDs.show();
		outDs.printSchema();
		// Select only the "name" column
		Dataset<Row> outDs1 = outDs;
		Dataset<Row> outDs2 = outDs1.select("a");
		outDs2.show();
		Dataset<Row> outDs3 = outDs;
		// Select all columns, but increment the b by 1
		outDs3.select(col("a"), col("b").plus(1)).show();
		Dataset<Row> outDs4 = outDs;
		outDs4.filter(col("b").gt(3)).show();
		
		//ctx.close();
		
		//SparkContext
		
		
		
	}

}
