package com.chnic.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

public class EmployeeSQLQuery {

    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder().appName("Employee Query by Spark SQL").getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);
        StructType schema = new StructType(new StructField[]{
                StructField.apply("id", IntegerType$.MODULE$, false, Metadata.empty()),
                StructField.apply("first_name", StringType$.MODULE$, false, Metadata.empty()),
                StructField.apply("title", StringType$.MODULE$, false, Metadata.empty()),
                StructField.apply("state", StringType$.MODULE$, true, Metadata.empty()),
                StructField.apply("laptop", StringType$.MODULE$, false, Metadata.empty()),
                StructField.apply("active", BooleanType$.MODULE$, false, Metadata.empty()),
        });

        Dataset<Row> dataset = sqlContext.read().option("header", true).schema(schema).csv(args[0]).cache();
        dataset.createTempView("employee");

        dataset.show();
        dataset.printSchema();

        dataset.sqlContext().sql(args[1]).write().mode(SaveMode.Overwrite).csv(args[2]);
    }
}
