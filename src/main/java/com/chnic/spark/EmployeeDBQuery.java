package com.chnic.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.col;

public class EmployeeDBQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Employee Database Query").master("local[*]").getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);

        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "root");
        properties.put("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> employeeDataSet = sqlContext.read().jdbc("jdbc:mysql://localhost:3306/test", "employee", properties).cache();
        employeeDataSet.show();

        employeeDataSet.select(col("title")).distinct().show();
        employeeDataSet.filter("title in ('CFO', 'CEO')").show();

        Dataset<Row> employeeMacDataSet = sqlContext.read().jdbc("jdbc:mysql://localhost:3306/test", "employee", new String[]{"laptop = 'MAC'"}, properties).cache();
        employeeMacDataSet.show();

        Dataset<Row> employeeMacDataSet2 = sqlContext.read().jdbc("jdbc:mysql://localhost:3306/test", "(select * from employee where state = 'PA') AS tmp_table", properties).cache();
        employeeMacDataSet2.show();
    }
}
