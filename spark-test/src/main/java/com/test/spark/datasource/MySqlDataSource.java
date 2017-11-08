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
          props.put("user", "APEX_040000");
          props.put("password", "emc@123");
          props.put("useSSL", "false");
          Dataset<Row> df = spark.read().jdbc("jdbc:oracle:thin://@192.168.1.26:1521:XE", "EMPLOYEE_RECORD", props);
          //df = df.orderBy(df.col("last_name"));
          df.show(false);
          df.printSchema();
     }
     
}
