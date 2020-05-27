package com.chnic.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SparkDistributedSharedOperation {

    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("RDD sample by Spark").setMaster("local[*]"));

        String words = "Free delivery on millions of items with Prime. Low prices across earth's biggest selection of books, music, DVDs, electronics, computers, software, apparel & accessories, shoes, jewelry, tools & hardware, housewares, furniture, sporting goods, beauty & personal care, groceries & just about anything else.";
        String[] arr = words.replaceAll(",", "").replaceAll("\\.", "").replaceAll("&", "").split(" ");

        Map<String, Integer> map = new HashMap<>();
        map.put("millions", 100000);
        map.put("biggest", 9999);
        map.put("low", 100);

        Broadcast<Map<String, Integer>> broadcast = sparkContext.broadcast(map);

        sparkContext.parallelize(Arrays.asList(arr), 4)
                .map(str -> new Tuple2<>(str.toLowerCase(), broadcast.getValue().getOrDefault(str.toLowerCase(), 0)))
                .sortBy(Tuple2::_2, false, 4)
                .collect()
                .forEach(System.out::println);

        LongAccumulator accumulator = sparkContext.sc().longAccumulator("hasA");
        sparkContext.parallelize(Arrays.asList(arr), 4)
                .foreach(s -> {
                    if (s.toLowerCase().contains("a")) {
                        accumulator.add(1);
                    }
                });
        System.out.println(accumulator.value());

        CollectionAccumulator<String> collectionAccumulator = sparkContext.sc().collectionAccumulator("col1");
        sparkContext.parallelize(Arrays.asList(arr), 4)
                .map(s -> {
                    if (s.toLowerCase().contains("b")) {
                        collectionAccumulator.add(s);
                    }
                    return s.toLowerCase();
                }).take(14);
        System.out.println(collectionAccumulator.value());

        CollectionAccumulator<String> collectionAccumulator2 = sparkContext.sc().collectionAccumulator("col2");
        sparkContext.parallelize(Arrays.asList(arr), 4)
                .map(s -> {
                    if (s.toLowerCase().contains("b")) {
                        collectionAccumulator2.add(s);
                    }
                    return s.toLowerCase();
                }).take(20);
        System.out.println(collectionAccumulator2.value());

        CollectionAccumulator<String> collectionAccumulator3 = sparkContext.sc().collectionAccumulator("col3");
        sparkContext.parallelize(Arrays.asList(arr), 4)
                .map(s -> {
                    if (s.toLowerCase().contains("b")) {
                        collectionAccumulator3.add(s);
                    }
                    return s.toLowerCase();
                }).collect();
        System.out.println(collectionAccumulator3.value());
    }
}
