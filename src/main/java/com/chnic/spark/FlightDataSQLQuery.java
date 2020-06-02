package com.chnic.spark;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class FlightDataSQLQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Flight Data SQL Query by Spark SQL").getOrCreate();

        sparkSession.sql("CREATE TABLE flight_summary(DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG) USING JSON OPTIONS (path '" + args[0] + "')");
        sparkSession.sql("SELECT * FROM flight_summary").show();

        sparkSession.sql("CREATE TABLE flight_summary_origin_usa USING parquet AS SELECT * FROM flight_summary WHERE ORIGIN_COUNTRY_NAME = 'United States'");
        sparkSession.sql("SELECT * FROM flight_summary_origin_usa").show();

        sparkSession.sql("CREATE TABLE flight_summary_origin_usa_partitioned USING parquet PARTITIONED BY (DEST_COUNTRY_NAME) AS SELECT * FROM flight_summary limit 10");
        sparkSession.sql("SELECT * FROM flight_summary_origin_usa_partitioned").show();

        sparkSession.sql("CREATE VIEW IF NOT EXISTS flight_summary_view AS SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) as country, count FROM flight_summary where count > 10 order by count desc");
        sparkSession.sql("SELECT count, country.ORIGIN_COUNTRY_NAME, country.DEST_COUNTRY_NAME FROM flight_summary_view").show();

        sparkSession.sql("CREATE VIEW IF NOT EXISTS flight_summary_dest_view AS SELECT DEST_COUNTRY_NAME, collect_list(count) as collected_counts FROM flight_summary flights GROUP BY DEST_COUNTRY_NAME");
        sparkSession.sql("SELECT DEST_COUNTRY_NAME, collected_counts[0] FROM flight_summary_dest_view").show();

        sparkSession.sql("DROP VIEW IF EXISTS flight_summary_dest_view");
        sparkSession.sql("DROP VIEW IF EXISTS flight_summary_view");
        sparkSession.sql("DROP TABLE IF EXISTS flight_summary_origin_usa_partitioned");
        sparkSession.sql("DROP TABLE IF EXISTS flight_summary_origin_usa");
        sparkSession.sql("DROP TABLE IF EXISTS flight_summary");

    }
}
