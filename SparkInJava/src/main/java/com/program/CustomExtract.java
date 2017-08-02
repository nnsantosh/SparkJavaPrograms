package com.program;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CustomExtract {

	
	public static void main(String[] args) {
		SparkSession spark = SparkSession
			      .builder().master("local")
			      .appName("Java Spark SQL data sources example")
			      .config("spark.some.config.option", "some-value")
			      .getOrCreate();
		// A JSON dataset is pointed to by path.
		// The path can be either a single text file or a directory storing text files.
		Dataset<Row> outDs = spark.read().json("data/test2.json");
		outDs.show();
		outDs.printSchema();
		
	}
}
