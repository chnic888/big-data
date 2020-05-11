package com.chnic.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

public class RetailDataStructuredQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Retail Data Query by Spark DataSet API").getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);

        StructType schema = new StructType(new StructField[]{
                StructField.apply("InvoiceNo", LongType$.MODULE$, false, Metadata.empty()),
                StructField.apply("StockCode", IntegerType$.MODULE$, false, Metadata.empty()),
                StructField.apply("Description", StringType$.MODULE$, false, Metadata.empty()),
                StructField.apply("Quantity", IntegerType$.MODULE$, true, Metadata.empty()),
                StructField.apply("InvoiceDate", TimestampType$.MODULE$, false, Metadata.empty()),
                StructField.apply("UnitPrice", DoubleType$.MODULE$, false, Metadata.empty()),
                StructField.apply("CustomerID", StringType$.MODULE$, true, Metadata.empty()),
                StructField.apply("Country", StringType$.MODULE$, false, Metadata.empty()),
        });

        Dataset<Row> dataset = sqlContext.read().option("header", true).option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .schema(schema).csv(args[0]).cache();

        dataset.printSchema();
        dataset.show();
    }
}
