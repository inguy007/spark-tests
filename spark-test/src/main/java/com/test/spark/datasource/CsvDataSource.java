package com.test.spark.datasource;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.test.spark.session.SparkSessionProvider;

public class CsvDataSource {
     
     public static void main(String[] args){
          SparkSession spark = SparkSessionProvider.getSparkSession();
          Map<String, String> optionMap = new HashMap<String, String>();
          optionMap.put("inferSchema", "false");
          optionMap.put("delimiter", ",");
          optionMap.put("header", "true");
          Dataset<Row> dataSet = spark.read().options(optionMap).csv("src/main/resources/data/inputCsvWithHeader.txt");
          dataSet.printSchema();
          dataSet.show();
          
          
          optionMap = new HashMap<String, String>();
          optionMap.put("inferSchema", "true");
          optionMap.put("delimiter", ",");
          optionMap.put("header", "false");
          dataSet = spark.read().options(optionMap).csv("src/main/resources/data/inputCsvWithoutHeader.txt");
          dataSet = dataSet.withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "name").withColumnRenamed("_c2", "dob").toDF();
          dataSet.printSchema();
          dataSet.show();
     }

}
