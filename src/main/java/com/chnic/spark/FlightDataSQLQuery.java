package com.chnic.spark;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class FlightDataSQLQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Flight Data SQL Query by Spark SQL").getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);

        sqlContext.sql("CREATE TABLE flight_summary(DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG) USING JSON OPTIONS (path '" + args[0] + "')");
        sqlContext.sql("SELECT * FROM flight_summary").show();

        sqlContext.sql("CREATE TABLE flight_summary_origin_usa USING parquet AS SELECT * FROM flight_summary WHERE ORIGIN_COUNTRY_NAME = 'United States'");
        sqlContext.sql("SELECT * FROM flight_summary_origin_usa").show();

        sqlContext.sql("CREATE TABLE flight_summary_origin_usa_partitioned USING parquet PARTITIONED BY (DEST_COUNTRY_NAME) AS SELECT * FROM flight_summary limit 10");
        sqlContext.sql("SELECT * FROM flight_summary_origin_usa_partitioned").show();

        sqlContext.sql("CREATE VIEW IF NOT EXISTS flight_summary_view AS SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) as country, count FROM flight_summary where count > 10 order by count desc");
        sqlContext.sql("SELECT count, country.ORIGIN_COUNTRY_NAME, country.DEST_COUNTRY_NAME FROM flight_summary_view").show();

        sqlContext.sql("CREATE VIEW IF NOT EXISTS flight_summary_dest_view AS SELECT DEST_COUNTRY_NAME, collect_list(count) as collected_counts FROM flight_summary flights GROUP BY DEST_COUNTRY_NAME");
        sqlContext.sql("SELECT DEST_COUNTRY_NAME, collected_counts[0] FROM flight_summary_dest_view").show();

        sqlContext.sql("DROP VIEW IF EXISTS flight_summary_dest_view");
        sqlContext.sql("DROP VIEW IF EXISTS flight_summary_view");
        sqlContext.sql("DROP TABLE IF EXISTS flight_summary_origin_usa_partitioned");
        sqlContext.sql("DROP TABLE IF EXISTS flight_summary_origin_usa");
        sqlContext.sql("DROP TABLE IF EXISTS flight_summary");

    }
}
