package com.chnic.spark;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class FlightDataStructuredQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Flight Data Query by Spark DataSet API").getOrCreate();

        Dataset<Row> dataset = sparkSession.read().option("inferSchema", "true").json(args[0]).cache();
        dataset.printSchema();
        dataset.show();

        String basePath = args[1];
        dataset.selectExpr("*").where(col("DEST_COUNTRY_NAME").equalTo("United States"))
                .write().mode(SaveMode.Overwrite).parquet(basePath + "/parquet");

        dataset.groupBy(col("DEST_COUNTRY_NAME")).agg(sum(col("count")).as("DEST_COUNTRY_COUNT"))
                .write().mode(SaveMode.Overwrite).format("avro").save(basePath + "/avro");

        dataset.groupBy(col("DEST_COUNTRY_NAME")).agg(sum(col("count")).as("DEST_COUNTRY_COUNT"))
                .where("DEST_COUNTRY_COUNT > 100").orderBy(col("DEST_COUNTRY_COUNT"))
                .write().mode(SaveMode.Overwrite).orc(basePath + "/orc");

        Column expr1 = col("ORIGIN_COUNTRY_NAME").equalTo("United States").and(col("DEST_COUNTRY_NAME").equalTo("China"));
        Column expr2 = col("ORIGIN_COUNTRY_NAME").equalTo("China").and(col("DEST_COUNTRY_NAME").equalTo("United States"));
        dataset.select(col("ORIGIN_COUNTRY_NAME"), col("DEST_COUNTRY_NAME"), col("count"))
                .where(expr1.or(expr2)).write().mode(SaveMode.Overwrite).option("header","true").csv(basePath + "/cvs");

        dataset.where(expr1.or(expr2)).withColumn("COUNTRIES", lit("USA2CHN"))
                .groupBy(col("COUNTRIES")).agg(sum(col("count")).as("SUM"))
                .select(col("COUNTRIES"), col("SUM"))
                .write().mode(SaveMode.Overwrite).json(basePath + "/json");
    }
}
