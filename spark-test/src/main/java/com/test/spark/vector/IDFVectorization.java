package com.test.spark.vector;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.NGram;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.test.spark.session.SparkSessionProvider;

public class IDFVectorization {

     public static void main(String[] args) {
          
          SparkSession spark = SparkSessionProvider.getSparkSession();
         /* List<Row> data = Arrays.asList(
                    RowFactory.create(0.0, "Hi I heard about Spark"),
                    RowFactory.create(0.0, "I wish Java could use case classes"),
                    RowFactory.create(1.0, "Logistic regression models are neat"));
          
          StructType schema = new StructType(new StructField[] {
                    new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                    new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
          });

          

          Dataset<Row> sentenceData = spark.createDataFrame(data, schema);*/

          /*Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
          Dataset<Row> wordsData = tokenizer.transform(sentenceData);
          wordsData.show(false);
          
          int numFeatures = 20;
          HashingTF hashingTF = new HashingTF()
                    .setInputCol("words")
                    .setOutputCol("rawFeatures")
                    .setNumFeatures(numFeatures);

          Dataset<Row> featurizedData = hashingTF.transform(wordsData);
          // alternatively, CountVectorizer can also be used to get term frequency vectors

          featurizedData.show(false);
          
          IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
          IDFModel idfModel = idf.fit(featurizedData);

          Dataset<Row> rescaledData = idfModel.transform(featurizedData);
          rescaledData.select("label", "features").show(false);*/
          
       // Input data: Each row is a bag of words from a sentence or document.
          /*List<Row> data = Arrays.asList(
            RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
            RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
            RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
          );
          StructType schema = new StructType(new StructField[]{
            new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
          });
          Dataset<Row> documentDF = spark.createDataFrame(data, schema);
          
       // Learn a mapping from words to Vectors.
          Word2Vec word2Vec = new Word2Vec()
            .setInputCol("text")
            .setOutputCol("result")
            .setVectorSize(10)
            .setMinCount(0);

          Word2VecModel model = word2Vec.fit(documentDF);
          Dataset<Row> result = model.transform(documentDF);

          for (Row row : result.collectAsList()) {
            List<String> text = row.getList(0);
            Vector vector = (Vector) row.get(1);
            System.out.println("Text: " + text + " => \nVector: " + vector + "\n");
          }*/
          
          
          List<Row> data = Arrays.asList(
                    RowFactory.create(0, Arrays.asList("Hi", "I", "heard", "about", "Spark")),
                    RowFactory.create(1, Arrays.asList("I", "wish", "Java", "could", "use", "case", "classes")),
                    RowFactory.create(2, Arrays.asList("Logistic", "regression", "models", "are", "neat"))
                  );

                  StructType schema = new StructType(new StructField[]{
                    new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                    new StructField(
                      "words", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
                  });

                  Dataset<Row> wordDataFrame = spark.createDataFrame(data, schema);

                  NGram ngramTransformer = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams");

                  Dataset<Row> ngramDataFrame = ngramTransformer.transform(wordDataFrame);
                  ngramDataFrame.select("ngrams").show(false);
          
          
     }

}
