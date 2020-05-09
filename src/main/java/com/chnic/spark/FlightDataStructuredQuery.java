package com.chnic.spark;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class FlightDataStructuredQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Flight Data Query by Spark DataSet API").getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);

        Dataset<Row> dataset = sqlContext.read().option("inferSchema", "true").json(args[0]).cache();
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
    }
}
