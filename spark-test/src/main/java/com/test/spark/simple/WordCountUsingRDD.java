package com.test.spark.simple;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.test.spark.session.JavaSparkContextProvider;

import scala.Tuple2;

public class WordCountUsingRDD {

     public static void main(String[] args) {
          JavaSparkContext jsc = JavaSparkContextProvider.getJavaSparkContext();
          JavaRDD<String> rdd = jsc.textFile("src/main/resources/data/sample-text.txt");
          JavaPairRDD<String, Integer> wordCounts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                    .mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);
          wordCounts.foreach(x -> System.out.println(x._1 + "=" + x._2));
     }
}
