package com.chnic.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

public class RetailDataStructuredQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Retail Data Query by Spark DataSet API").getOrCreate();

        StructType schema = new StructType(new StructField[]{
                StructField.apply("InvoiceNo", StringType$.MODULE$, false, Metadata.empty()),
                StructField.apply("StockCode", IntegerType$.MODULE$, false, Metadata.empty()),
                StructField.apply("Description", StringType$.MODULE$, false, Metadata.empty()),
                StructField.apply("Quantity", IntegerType$.MODULE$, true, Metadata.empty()),
                StructField.apply("InvoiceDate", TimestampType$.MODULE$, false, Metadata.empty()),
                StructField.apply("UnitPrice", DoubleType$.MODULE$, false, Metadata.empty()),
                StructField.apply("CustomerID", StringType$.MODULE$, true, Metadata.empty()),
                StructField.apply("Country", StringType$.MODULE$, false, Metadata.empty()),
        });

        Dataset<Row> dataset = sparkSession.read().option("header", true).option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .schema(schema).csv(args[0]).cache();

        dataset.printSchema();
        dataset.show();

        String basePath = args[1];
        dataset.select(coalesce(col("CustomerID"), col("Description"))).write().mode(SaveMode.Overwrite).option("header", "true").csv(basePath + "/coalesce");

        dataset.na().drop("any").write().mode(SaveMode.Overwrite).option("header", "true").csv(basePath + "/drop-any");
        dataset.na().drop("all").write().mode(SaveMode.Overwrite).option("header", "true").csv(basePath + "/drop-all");

        Dataset<Row> invoiceDataSet = dataset.select(struct(col("InvoiceNo"), col("InvoiceDate")).as("InvoiceInfo"), col("Quantity"));
        invoiceDataSet.select(col("InvoiceInfo").getField("InvoiceNo"), col("InvoiceInfo").getField("InvoiceDate"), col("Quantity"))
                .write().mode(SaveMode.Overwrite).option("header", "true").csv(basePath + "/struct");

        dataset.select(split(col("Description"), " ").as("DescriptionArray")).selectExpr("DescriptionArray[0]")
                .write().mode(SaveMode.Overwrite).option("header", "true").csv(basePath + "/array");

        dataset.select(map(col("InvoiceNo"), col("InvoiceDate")).as("InvoiceMap")).selectExpr("InvoiceMap['581578']").na().drop("all")
                .write().mode(SaveMode.Overwrite).option("header", "true").csv(basePath + "/map");

        dataset.select(struct(struct(col("InvoiceNo"), col("InvoiceDate")).as("InvoiceInfo"), split(col("Description"), " ").as("Descriptions"), col("CustomerID"), col("Country")).as("RetailData"))
                .select(to_json(col("RetailData")).as("RetailDataJson"))
                .select(get_json_object(col("RetailDataJson"), "$.CustomerID").as("CusID"),
                        get_json_object(col("RetailDataJson"), "$.Descriptions[0]").as("Des"),
                        json_tuple(col("RetailDataJson"), "Country"))
                .na().drop("any")
                .where("Country = 'Norway'")
                .write().mode(SaveMode.Overwrite).json(basePath + "/json");

        UserDefinedFunction udfLowerCase = udf(
                (UDF1<String, String>) String::toLowerCase, StringType$.MODULE$
        );
        dataset.select(col("Description")).na().drop("any")
                .select(udfLowerCase.apply(col("Description")))
                .write().mode(SaveMode.Overwrite).option("header", "true").csv(basePath + "/udf/func");

        sparkSession.udf().register("udfLowerCaseExpr", (UDF1<String, String>) String::toLowerCase, StringType$.MODULE$);
        dataset.select(col("Description")).na().drop("any")
                .selectExpr("udfLowerCaseExpr(Description)")
                .write().mode(SaveMode.Overwrite).option("header", "true").csv(basePath + "/udf/expr");
    }
}
