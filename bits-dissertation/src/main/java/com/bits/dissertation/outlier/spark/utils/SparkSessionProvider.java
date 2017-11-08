package com.bits.dissertation.outlier.spark.utils;

import org.apache.spark.sql.SparkSession;

/**
 * The entry point into all functionality in Spark is the SparkSession class. To create a basic SparkSession, just use
 * SparkSession.builder()
 *
 */
public class SparkSessionProvider {

     private static SparkSession sparkSession;

     static {
          sparkSession = SparkSession.builder().appName("My_App").master("local").getOrCreate();
     }

     public static SparkSession getSparkSession() {
          return sparkSession;
     }

}
