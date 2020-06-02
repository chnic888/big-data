package com.chnic.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.window;

public class SparkStreamingEventQuery {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("Spark streaming Event Demo").getOrCreate();
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

//        streaming.selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")
//                .groupBy(window(column("event_time"), "10 minutes"))
//                .count()
//                .writeStream()
//                .queryName("events_per_window")
//                .format("console")
//                .option("truncate", "false")
//                .outputMode(OutputMode.Complete())
//                .start()
//                .awaitTermination();

//        streaming.selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")
//                .groupBy(window(column("event_time"), "10 minutes", "5 minutes"))
//                .count()
//                .writeStream()
//                .queryName("events_per_window")
//                .format("console")
//                .option("truncate", "false")
//                .outputMode(OutputMode.Complete())
//                .start()
//                .awaitTermination();

//        streaming.selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")
//                .withWatermark("event_time", "30 minutes")
//                .groupBy(window(column("event_time"), "10 minutes", "5 minutes"))
//                .count()
//                .writeStream()
//                .queryName("events_per_window")
//                .format("console")
//                .option("truncate", "false")
//                .outputMode(OutputMode.Complete())
//                .start()
//                .awaitTermination();

        streaming.selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")
                .withWatermark("event_time", "5 seconds")
                .dropDuplicates("User", "event_time")
                .groupBy("User")
                .count()
                .writeStream()
                .queryName("deduplicated")
                .format("console")
                .option("truncate", "false")
                .outputMode(OutputMode.Complete())
                .start()
                .awaitTermination();
    }
}
