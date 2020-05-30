package com.chnic.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

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

//        streaming.groupBy(col("gt")).count()
//                .writeStream().queryName("activity_counts").format("console").outputMode("complete")
//                .start()
//                .awaitTermination();

//        streaming.withColumn("stairs", expr("gt like '%stairs%'"))
//                .where("stairs")
//                .where("gt is not null")
//                .select("gt", "Model", "Arrival_Time", "Creation_Time")
//                .writeStream().queryName("simple_transform").format("console").outputMode("append")
//                .start()
//                .awaitTermination();

//        streaming.cube(col("gt"), col("Model")).avg("x", "y", "z")
//                .writeStream().queryName("device_counts").format("console").outputMode("complete")
//                .start()
//                .awaitTermination();

        streaming.cube(col("gt"), col("Model")).avg("x", "y", "z")
                .writeStream().trigger(Trigger.ProcessingTime("5 seconds")).queryName("device_counts").format("console").outputMode("complete")
                .start()
                .awaitTermination();

//        streaming.writeStream().queryName("foreach sink").foreach(new ForeachWriter<Row>() {
//            @Override
//            public boolean open(long partitionId, long epochId) {
//                System.out.println(partitionId + " : " + epochId);
//                return true;
//            }
//
//            @Override
//            public void close(Throwable errorOrNull) {
//
//            }
//
//            @Override
//            public void process(Row value) {
//                System.out.println(value.json());
//            }
//        }).start().awaitTermination();
    }
}
