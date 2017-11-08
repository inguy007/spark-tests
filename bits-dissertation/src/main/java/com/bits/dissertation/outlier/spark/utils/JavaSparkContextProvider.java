package com.bits.dissertation.outlier.spark.utils;

import org.apache.spark.api.java.JavaSparkContext;

public class JavaSparkContextProvider {
     
     private static JavaSparkContext javaSparkContext;
     
     static{
          javaSparkContext = new JavaSparkContext("local", "My_App");
     }
     
     public static JavaSparkContext getJavaSparkContext(){
          return javaSparkContext;
     }

}
