package com.chnic.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class FlightDataStructuredQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Flight Data Query by Spark DataSet API").getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);

        Dataset<Row> dataset = sqlContext.read().option("inferSchema", "true").json(args[0]).cache();
        dataset.printSchema();
        dataset.show();

        dataset.selectExpr("*").where(col("DEST_COUNTRY_NAME").equalTo("United States")).show();
        dataset.groupBy(col("DEST_COUNTRY_NAME")).agg(sum(col("count")).as("DEST_COUNTRY_COUNT")).show();
        dataset.groupBy(col("DEST_COUNTRY_NAME")).agg(sum(col("count")).as("DEST_COUNTRY_COUNT"))
                .where("DEST_COUNTRY_COUNT > 100").orderBy(col("DEST_COUNTRY_COUNT")).show(100);
    }
}
