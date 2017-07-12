package com.test.spark.simple;

import java.util.Arrays;

import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.test.spark.session.SparkSessionProvider;

public class WordCount {

     public static void main(String[] args) {
          /* JavaSparkContext jsc = new JavaSparkContext("local", "spark-test");
          JavaRDD<String> fileContentRDD = jsc.textFile("src/main/resources/data/sample-text.txt");
          List<String[]> stringList = fileContentRDD.flatMap(text->text.split(" ").).collect();*/

          SparkSession spark = SparkSessionProvider.getSparkSession();
          Dataset<String> df = spark.read().text("src/main/resources/data/sample-text.txt").as(Encoders.STRING());
          System.out.println("Using map method on data frame");
          
          
          Dataset<String> words = df.flatMap(s -> {
               return Arrays.asList(s.toLowerCase().split(" ")).iterator();
          }, Encoders.STRING())
                    .filter(s -> !s.isEmpty())
                    .coalesce(1); 
          Dataset<Row> t = words.groupBy("value") 
                    .count()
                    .toDF("word", "count");
          t.show(100000, false);
     }

}
