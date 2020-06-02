package com.chnic.spark;

import org.apache.spark.sql.*;

import java.util.Properties;

import static org.apache.spark.sql.functions.col;

public class EmployeeDBQuery {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Employee Database Query").master("local[*]").getOrCreate();

        String url = args[0];
        Properties properties = new Properties();
        properties.put("user", args[1]);
        properties.put("password", args[2]);
        properties.put("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> employeeDataSet = sparkSession.read().jdbc(url, "employee", properties).cache();
        employeeDataSet.show();

        employeeDataSet.select(col("title")).distinct().show();
        employeeDataSet.filter("title in ('CFO', 'CEO')").show();

        Dataset<Row> employeeMacDataSet = sparkSession.read().jdbc(url, "employee", new String[]{"laptop = 'MAC'"}, properties).cache();
        employeeMacDataSet.show();

        Dataset<Row> employeeMacDataSet2 = sparkSession.read().jdbc(url, "(select * from employee where state = 'PA') AS tmp_table", properties).cache();
        employeeMacDataSet2.show();

        employeeMacDataSet.write().mode(SaveMode.Overwrite).jdbc(url, "employee_bak", properties);
    }
}
