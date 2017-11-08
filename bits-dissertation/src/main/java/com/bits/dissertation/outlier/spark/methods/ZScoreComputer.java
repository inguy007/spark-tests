package com.bits.dissertation.outlier.spark.methods;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import com.bits.dissertation.outlier.spark.utils.SparkSessionProvider;

import scala.collection.Seq;

public class ZScoreComputer {
     
     public static void main(String[] args) {
          SparkSession sparkSession = SparkSessionProvider.getSparkSession();
          Dataset<Row> input = sparkSession.read().option("header", "true").option("delimiter", "|")
                    .csv("src/main/resources/demo_outlier_sample.txt");
          input.show();
          Dataset<Row> partitionedInput = input.repartition(input.col("customer_id"));
          Dataset<Row> groupedByAmount = partitionedInput.groupBy(partitionedInput.col("customer_id"))
                    .agg(org.apache.spark.sql.functions.collect_list(partitionedInput.col("amount")).alias("amounts"));
          groupedByAmount.show();

          // Generate Mean
          sparkSession.udf().register("udfGenerateMean", new UDF1<Seq<String>, Double>() {
               private static final long serialVersionUID = 1L;

               @Override
               public Double call(Seq<String> amountSeq) throws Exception {
                    Double sum = 0.0;
                    List<String> amountList = scala.collection.JavaConversions.seqAsJavaList(amountSeq);
                    for (String value : amountList) {
                         sum = sum + Double.parseDouble(value);
                    }
                    Double mean = sum/amountList.size();
                    return mean;
               }

          }, DataTypes.DoubleType);

          groupedByAmount.createOrReplaceTempView("groupedByAmount");
          Dataset<Row> groupedByAmountWithMean = sparkSession
                    .sql("SELECT customer_id, amounts, udfGenerateMean(amounts) AS mean FROM groupedByAmount");
          groupedByAmountWithMean.show();

          // Generate Standard Deviation
          sparkSession.udf().register("udfGenerateStandardDeviation", new UDF2<Seq<String>, Double, Double>() {
               private static final long serialVersionUID = 1L;

               @Override
               public Double call(Seq<String> amountSeq, Double mean) throws Exception {
                    Double sum = 0.0;
                    List<String> amountList = scala.collection.JavaConversions.seqAsJavaList(amountSeq);
                    for (String value : amountList) {
                         Double meanDifferential = Double.parseDouble(value) - mean;
                         Double meanDifferentialSquare = Math.pow(meanDifferential, 2);
                         sum = sum + meanDifferentialSquare;
                    }
                    Double meanDifferentialSquaredAverage = sum / amountList.size();
                    Double standardDeviation = Math.sqrt(meanDifferentialSquaredAverage);
                    return standardDeviation;
               }

          }, DataTypes.DoubleType);
          
          groupedByAmountWithMean.createOrReplaceTempView("groupedByAmountWithMean");
          Dataset<Row> groupedByAmountWithMeanAndSD = sparkSession
                    .sql("SELECT customer_id AS grouped_customer_id, amounts, mean, udfGenerateStandardDeviation(amounts,mean) AS standard_deviation  FROM groupedByAmountWithMean");
          groupedByAmountWithMeanAndSD.show();
          
          //Generate ZScore
          Dataset<Row> inputWithMeanAndSD = input.join(groupedByAmountWithMeanAndSD, input.col("customer_id").equalTo(groupedByAmountWithMeanAndSD.col("grouped_customer_id")), "inner");
          
          sparkSession.udf().register("udfGenerateZScore", new UDF3<String, Double, Double, Double>() {
               private static final long serialVersionUID = 1L;


               @Override
               public Double call(String amount, Double mean, Double standardDeviation) throws Exception {
                    Double zScore = (Double.parseDouble(amount)-mean)/standardDeviation;
                    return zScore;
               }

          }, DataTypes.DoubleType);
          
          inputWithMeanAndSD.createOrReplaceTempView("inputWithMeanAndSD");
          Dataset<Row> inputWithMeanSDAndZScore = sparkSession
                    .sql("SELECT transaction_id, customer_id, amount, time, purpose, mean, standard_deviation, udfGenerateZScore(amount,mean,standard_deviation) AS zscore FROM inputWithMeanAndSD");
          inputWithMeanSDAndZScore.show();
          closeSparkSession(sparkSession);

     }
     
     public static SparkSession initializeSparkSession(String applicationName) {
          SparkConf conf;
          SparkContext sc;
          SparkSession sparkSession = null;
          try {
               sparkSession = SparkSession.builder().appName(applicationName).master("local")
                         .config("spark.sql.streaming.checkpointLocation", "src/main/resources/checkpoint").getOrCreate();

          } catch (Exception e) {
               System.out.println("ERROR creating SparkSession" + e.getMessage());
          }
          return sparkSession;
     }

     public static void closeSparkSession(SparkSession sparkSession) {
          sparkSession.catalog().clearCache();
          sparkSession.stop();
     }
}
