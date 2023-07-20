package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.SaveMode;


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
        dataset=dataset.filter(functions.col("Receiving Country Code").isin("PL", "FR","BE"));
        dataset.groupBy("Receiving Country Code", "Sending Country Code")
                .count()
                .orderBy(functions.col("Receiving Country Code").desc())
                .show(100);
        saveData(dataset,"PL","Polonia");
        saveData(dataset,"FR","Franta");
        saveData(dataset,"ES","Belgia");
    }

    public static void saveData(Dataset<Row> dataset, String countryCode, String tableName) {
        dataset
                .filter(col("Receiving Country Code").isin(countryCode))
                .groupBy("Receiving Country Code", "Sending Country Code")
                .count().orderBy("Receiving Country Code", "Sending Country Code")
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://127.0.0.1:3306/tema")
                .option("dbtable", tableName)
                .option("user", "root")
                .option("password", "Ade1234-")
                .save(tableName + ".tema");
    }
}