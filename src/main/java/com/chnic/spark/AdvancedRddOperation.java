package com.chnic.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class AdvancedRddOperation {

    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("RDD sample by Spark").setMaster("local[*]"));
        String str = "COVID-19 is caused by a coronavirus called SARS-CoV-2. Older adults and people who have severe underlying medical conditions like heart or lung disease or diabetes seem to be at higher risk for developing more serious complications from COVID-19 illness.";
        List<String> data = Arrays.asList(str.split(" "));

        sparkContext.parallelize(data)
                .map(key -> new Tuple2<>(key.toLowerCase(), 1))
                .keyBy(t -> t._1().toLowerCase().substring(0, 1))
                .mapValues(t -> t._1().toUpperCase())
                .collect()
                .forEach(System.out::println);

        sparkContext.parallelize(data)
                .map(key -> new Tuple2<>(key.toLowerCase(), 1))
                .keyBy(t -> t._1().toLowerCase().substring(0, 1))
                .mapValues(t -> t._1().toUpperCase())
                .lookup("s")
                .forEach(System.out::println);

        sparkContext.parallelize(data)
                .map(key -> new Tuple2<>(key.toLowerCase(), 1))
                .keyBy(t -> t._1().toLowerCase().substring(0, 1))
                .mapValues(t -> t._1().toUpperCase())
                .countByKey()
                .forEach((k, v) -> System.out.println(k + " - " + v));

        sparkContext.parallelize(data)
                .map(key -> new Tuple2<>(key.toLowerCase(), 1))
                .keyBy(t -> t._1().toLowerCase().substring(0, 1))
                .mapValues(t -> t._1().toUpperCase())
                .groupByKey()
                .foreach(p -> System.out.println(p._1() + " - " + p._2()));
    }
}
