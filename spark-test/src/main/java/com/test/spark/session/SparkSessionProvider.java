package com.test.spark.session;

import org.apache.spark.sql.SparkSession;

public class SparkSessionProvider {
     
     private static SparkSession sparkSession;
     
     static{
          sparkSession = SparkSession.builder().appName("My_App").master("local").getOrCreate();
     }
     
     public static SparkSession getSparkSession(){
          return sparkSession;
     }

}
