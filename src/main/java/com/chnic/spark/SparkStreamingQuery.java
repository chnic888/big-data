package com.chnic.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

public class SparkStreamingQuery {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("Spark streaming demo").getOrCreate();
        spark.sparkContext().setLogLevel("error");

        StructType schema = new StructType(new StructField[]{
                StructField.apply("Arrival_Time", LongType$.MODULE$, true, Metadata.empty()),
                StructField.apply("Creation_Time", LongType$.MODULE$, true, Metadata.empty()),
                StructField.apply("Device", StringType$.MODULE$, true, Metadata.empty()),
                StructField.apply("Index", LongType$.MODULE$, true, Metadata.empty()),
                StructField.apply("Model", StringType$.MODULE$, true, Metadata.empty()),
                StructField.apply("User", StringType$.MODULE$, true, Metadata.empty()),
                StructField.apply("_corrupt_record", StringType$.MODULE$, true, Metadata.empty()),
                StructField.apply("gt", StringType$.MODULE$, true, Metadata.empty()),
                StructField.apply("x", DoubleType$.MODULE$, true, Metadata.empty()),
                StructField.apply("y", DoubleType$.MODULE$, true, Metadata.empty()),
                StructField.apply("z", DoubleType$.MODULE$, true, Metadata.empty()),
        });

        Dataset<Row> streaming = spark.readStream().schema(schema).option("maxFilesPerTrigger", 1).json(args[0]);
        Dataset<Row> activityCounts = streaming.groupBy(col("gt")).count();
        StreamingQuery activityQuery = activityCounts.writeStream().queryName("activity_counts").format("console").outputMode("complete").start();
        activityQuery.awaitTermination();
    }
}
