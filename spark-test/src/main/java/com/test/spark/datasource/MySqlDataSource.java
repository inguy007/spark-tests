package com.test.spark.datasource;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.test.spark.session.SparkSessionProvider;

public class MySqlDataSource {

     public static void main(String[] args){
          
          SparkSession spark = SparkSessionProvider.getSparkSession();
          
          Properties props = new Properties();
          props.put("user", "root");
          props.put("password", "root");
          props.put("useSSL", "false");
          Dataset<Row> df = spark.read().jdbc("jdbc:mysql://localhost:3306/bedrock44x", "system_config", props);
          //df = df.orderBy(df.col("last_name"));
          df.show(false);
          df.printSchema();
     }
     
}
