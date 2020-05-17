package com.chnic.spark;

import org.apache.spark.sql.*;

public class PersonJoinQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Person Join by Spark SQL").getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);

        String sourceBasePath = args[0];
        Dataset<Row> personDataSet = sqlContext.read().option("inferSchema", "true").json(sourceBasePath + "/person.json").cache();
        Dataset<Row> graduateProgramDataSet = sqlContext.read().option("inferSchema", "true").json(sourceBasePath + "/graduate-program.json").cache();
        Dataset<Row> sparkStatusDataSets = sqlContext.read().option("inferSchema", "true").json(sourceBasePath + "/spark-status.json").cache();

        personDataSet.printSchema();
        graduateProgramDataSet.printSchema();
        sparkStatusDataSets.printSchema();

        personDataSet.show();
        graduateProgramDataSet.show();
        sparkStatusDataSets.show();

        String basePath = args[1];
        personDataSet.join(graduateProgramDataSet, personDataSet.col("graduate_program").equalTo(graduateProgramDataSet.col("id")), "inner")
                .drop(graduateProgramDataSet.col("id"))
                .write().mode(SaveMode.Overwrite).json(basePath + "/inner");

    }
}
