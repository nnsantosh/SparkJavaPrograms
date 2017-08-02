package com.program;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

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
		
		Dataset<Row> outDs2 = outDs.select("glossary");
		outDs2.show();
		outDs2.printSchema();
		
		Dataset<Row> outDs3 = outDs2.select("glossary.title");
		
		SQLContext sqlContext = spark.sqlContext();
		
		sqlContext.udf().register("udfUppercase",
				  (String string) -> string.toUpperCase(), DataTypes.StringType);

		Dataset<Row> modifiedDs = outDs3.select(callUDF("udfUppercase", col("title")));
		modifiedDs.show();
		modifiedDs.printSchema();
		
	}
}
