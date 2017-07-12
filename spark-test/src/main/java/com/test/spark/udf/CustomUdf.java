package com.test.spark.udf;

import static org.apache.spark.sql.functions.callUDF;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import com.test.spark.session.SparkSessionProvider;

public class CustomUdf {

     public static void main(String[] args) {

          SparkSession spark = SparkSessionProvider.getSparkSession();

          spark.udf().register("AgeUdf", x -> {
               Date currentDate = new Date();
               SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
               Date dateOfBirth = sdf.parse((String) x);
               return currentDate.getYear() - dateOfBirth.getYear();
          }, DataTypes.IntegerType);
          
          Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true")
                    .option("header", "true").load("src/main/resources/data/input.txt");

          df = df.withColumn("age", callUDF("AgeUdf", df.col("dob").cast(DataTypes.StringType)));

          df.show();
     }

}
