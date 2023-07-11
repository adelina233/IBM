package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("C:/Users/40759/IdeaProjects/untitled10/src/main/resources/erasmus.csv");

        dataset.printSchema();
        dataset.select("Mobility Duration").show();
        dataset.select(functions.lower(functions.col("Sending Country Code")), functions.col("Participant Age")).show();
        dataset.filter(functions.col("Participant Age").$greater(25)).show();
        dataset.groupBy("Participant Age").count().show();
        dataset.groupBy("Receiving Country Code", "Sending Country Code")
                .count()
                .withColumnRenamed("count", "Number Of Students")
                .orderBy(functions.col("Receiving Country Code").desc())
                .show();
    }
}
