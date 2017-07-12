package com.test.spark.vector;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.test.spark.session.SparkSessionProvider;

public class StopWords {

     public static void main(String[] args) {
          SparkSession spark = SparkSessionProvider.getSparkSession();
          StopWordsRemover remover = new StopWordsRemover()
                    .setInputCol("raw")
                    .setOutputCol("filtered");

          System.out.println(Arrays.toString(remover.getStopWords()));
          
          List<Row> data = Arrays.asList(
                    RowFactory.create(Arrays.asList("I", "saw", "the", "red", "balloon")),
                    RowFactory.create(Arrays.asList("Mary", "had", "a", "little", "lamb")));

          StructType schema = new StructType(new StructField[] {
                    new StructField(
                              "raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
          });

          Dataset<Row> dataset = spark.createDataFrame(data, schema);
          remover.transform(dataset).show(false);
     }

}
