package spark.sql.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();
        Dataset<Row> data = spark.read().option("header", true).csv("text.csv");
        data.show();
        System.out.println("------------------------");
        data.printSchema();
        System.out.println("------------------------");
        data.select("ten").show();
        System.out.println("------------------------");
        data.filter(data.col("ten").$greater("tuan")).show();
        System.out.println("------------------------");
        data.groupBy("id").agg(functions.collect_list("ten").as("ten")).show();
        System.out.println("------------------------");
        data.createOrReplaceTempView("data");
        spark.sql("select id, any_value(ten) from data group by id").show();
    }
}