package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Read_CSV")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .load("C:/Users/40759/IdeaProjects/untitled10/src/main/resources/erasmus.csv");

        dataset.show();
    }
}
