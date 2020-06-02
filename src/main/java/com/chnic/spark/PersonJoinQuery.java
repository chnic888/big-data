package com.chnic.spark;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class PersonJoinQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Person Join by Spark SQL").getOrCreate();

        String sourceBasePath = args[0];
        Dataset<Row> personDataSet = sparkSession.read().option("inferSchema", "true").json(sourceBasePath + "/person.json").cache();
        Dataset<Row> graduateProgramDataSet = sparkSession.read().option("inferSchema", "true").json(sourceBasePath + "/graduate-program.json").cache();
        Dataset<Row> sparkStatusDataSets = sparkSession.read().option("inferSchema", "true").json(sourceBasePath + "/spark-status.json").cache();

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

        personDataSet.join(graduateProgramDataSet, personDataSet.col("graduate_program").equalTo(graduateProgramDataSet.col("id")), "outer")
                .drop(graduateProgramDataSet.col("id"))
                .write().mode(SaveMode.Overwrite).json(basePath + "/outer");

        personDataSet.join(graduateProgramDataSet, personDataSet.col("graduate_program").equalTo(graduateProgramDataSet.col("id")), "left_outer")
                .drop(graduateProgramDataSet.col("id"))
                .write().mode(SaveMode.Overwrite).json(basePath + "/left_outer");

        personDataSet.join(graduateProgramDataSet, personDataSet.col("graduate_program").equalTo(graduateProgramDataSet.col("id")), "right_outer")
                .drop(graduateProgramDataSet.col("id"))
                .write().mode(SaveMode.Overwrite).json(basePath + "/right_outer");

        personDataSet.join(graduateProgramDataSet, personDataSet.col("graduate_program").equalTo(graduateProgramDataSet.col("id")), "left_semi")
                .write().mode(SaveMode.Overwrite).json(basePath + "/left_semi");

        personDataSet.join(graduateProgramDataSet, personDataSet.col("graduate_program").equalTo(graduateProgramDataSet.col("id")), "left_anti")
                .write().mode(SaveMode.Overwrite).json(basePath + "/left_anti");

        personDataSet.crossJoin(graduateProgramDataSet).drop(graduateProgramDataSet.col("id"))
                .write().mode(SaveMode.Overwrite).json(basePath + "/cross");

        personDataSet.withColumnRenamed("id", "personId").join(sparkStatusDataSets, expr("array_contains(spark_status, id)"))
                .withColumnRenamed("id", "spark_status_id")
                .write().mode(SaveMode.Overwrite).json(basePath + "/complex");
    }
}
