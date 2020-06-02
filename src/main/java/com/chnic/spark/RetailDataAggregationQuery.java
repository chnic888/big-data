package com.chnic.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class RetailDataAggregationQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Retail Data Aggregation Query by Spark DataSet API").getOrCreate();

        Dataset<Row> dataset = sparkSession.read().option("header", true).option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .option("inferSchema", "true").csv(args[0]).cache();

        dataset.printSchema();
        dataset.show();

        String basePath = args[1];
        WindowSpec windowSpec = Window.partitionBy(col("CustomerID"), col("Date")).orderBy(col("Quantity").desc()).rowsBetween(Window.unboundedPreceding(), Window.currentRow());
        dataset.withColumn("Date", to_date(col("InvoiceDate"), "yyyy-MM-dd"))
                .where("CustomerID IS NOT NULL").orderBy(col("CustomerID"))
                .select(
                        col("CustomerID"),
                        col("Date"),
                        col("Quantity"),
                        rank().over(windowSpec).as("QuantityRank"),
                        dense_rank().over(windowSpec).as("QuantityDenseRank"),
                        max(col("Quantity")).over(windowSpec).as("QuantityMax")
                ).write().mode(SaveMode.Overwrite).option("header", "true").csv(basePath + "/window");

        dataset.na().drop().withColumn("Date", to_date(col("InvoiceDate"), "yyyy/mm/dd"))
                .rollup(col("Date"), col("Country")).agg(sum(col("Quantity")).as("Quantity_Sum"))
                .select(col("Date"), col("Country"), col("Quantity_Sum"))
                .orderBy(col("Date"))
                .write().mode(SaveMode.Overwrite).option("header", "true").csv(basePath + "/rollup");

        dataset.na().drop().withColumn("Date", to_date(col("InvoiceDate"), "yyyy/mm/dd"))
                .cube(col("Date"), col("Country"), col("CustomerID")).agg(sum(col("Quantity")).as("Quantity_Sum"))
                .select(col("Date"), col("Country"), col("CustomerID"), col("Quantity_Sum"))
                .orderBy(col("Date"))
                .write().mode(SaveMode.Overwrite).option("header", "true").csv(basePath + "/cube");
    }
}
