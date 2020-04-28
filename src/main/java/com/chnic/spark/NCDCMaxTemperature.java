package com.chnic.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NCDCMaxTemperature {

    private static final Pattern pattern = Pattern.compile("^.{15}(\\d{4}).{68}([+-]\\d{4})(\\d).*$");

    public static void main(String[] args) {
        new JavaSparkContext(new SparkConf().setAppName("Compute Max Temperature by Spark")).textFile(args[0])
                .filter(line -> pattern.matcher(line).matches())
                .map(line -> {
                    Matcher matcher = pattern.matcher(line);
                    boolean matches = matcher.matches();
                    int year = Integer.parseInt(matcher.group(1));
                    int temperature = Integer.parseInt(matcher.group(2));
                    String qualityCode = matcher.group(3);

                    return new String[]{String.valueOf(year), String.valueOf(temperature), qualityCode};
                })
                .filter(arr -> !arr[1].equals("9999") && arr[2].matches("[01459]"))
                .mapToPair(arr -> new Tuple2<>(Integer.parseInt(arr[0]), (float) (Integer.parseInt(arr[1]) / 10.0)))
                .reduceByKey(Math::max)
                .map(t -> String.format("%d\t%.1f", t._1, t._2))
                .saveAsTextFile(args[1]);
    }
}
