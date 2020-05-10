package com.chnic.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class RetailDataStructuredQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Retail Data Query by Spark DataSet API").getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);

        Dataset<Row> dataset = sqlContext.read().option("header", true).option("inferSchema", "true").csv(args[0]).cache();
        dataset.printSchema();
        dataset.show();
    }
}
