package com.bits.dissertation.outlier.detection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StringType;

import com.bits.dissertation.outlier.schema.RecordSchemaVO;
import com.bits.dissertation.outlier.spark.utils.BucketGenerator;

import scala.Tuple2;

public class HistogramBasedOutlierScoreGenerator implements OutlierScoreGenerator {

     /**
      * 
      */
     private static final long serialVersionUID = 4604508947570136546L;

     @Override
     public void generateOutlierScores(SparkSession session, Dataset<Row> dataset,
               List<RecordSchemaVO> concernedColumns, Column[] categoricalColumns, Column[] featureColumns) {

          List<Tuple2<String, Map<String, Double>>> outlierScores = dataset.toJavaRDD().mapToPair(row -> {
               String key = "";
               String identifierValue = "";
               String recordValue = "";
               BucketGenerator bucketGenerator = new BucketGenerator();
               for (int i = 0; i < concernedColumns.size(); i++) {
                    RecordSchemaVO recordSchema = concernedColumns.get(i);
                    if (recordSchema.isCategorical()) {
                         if (StringUtils.isBlank(key)) {
                              key = String.valueOf(row.get(recordSchema.getPosition()));
                         } else {
                              key = "#" + String.valueOf(row.get(recordSchema.getPosition()));
                         }

                    }
                    else if (recordSchema.isIdentifier()) {
                         identifierValue = String.valueOf(row.get(recordSchema.getPosition()));
                    }else if (recordSchema.isFeature()) {
                         recordValue = String.valueOf(row.get(recordSchema.getPosition()));
                         bucketGenerator.addBucketKey(bucketGenerator.getBucket(recordValue, recordSchema));
                    }
               }
               String bucketKeyValue = bucketGenerator.getBucketKey();
               return new Tuple2<>(key, identifierValue + ":" + bucketKeyValue);
          }).groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, Map<String,Double>>() {

               @Override
               public Iterator<Tuple2<String, Map<String, Double>>> call(Tuple2<String, Iterable<String>> t)
                         throws Exception {
                    
                    System.out.println("Processing for CATEGORY :"+t._1);
                    Map<String,List<String>> bucketToIdentifierMap = new HashMap<>();
                     
                    for(String s : t._2){
                         String[] identifierBucket = s.split(":");
                         String identifier = identifierBucket[0];
                         String bucket = identifierBucket[1];
                         if(bucketToIdentifierMap.get(bucket) != null){
                              bucketToIdentifierMap.get(bucket).add(identifier);
                         }else{
                              List<String> identifierList = new ArrayList<>();
                              identifierList.add(identifier);
                              bucketToIdentifierMap.put(bucket, identifierList);
                         }
                    }
                    System.out.println("Bucket to Identifiers Map :"+bucketToIdentifierMap);
                    Map<String,Double> outlierScoreMap = new HashMap<>();
                    for(Entry<String, List<String>> bucketIdentifierEntry : bucketToIdentifierMap.entrySet()){
                         List<String> identifiersInBucket = bucketIdentifierEntry.getValue();
                         int totalIdentifiers = identifiersInBucket.size();
                         double outlierScoreOfBucket = (double)1/(double)totalIdentifiers;
                         for(String identifer : identifiersInBucket){
                              outlierScoreMap.put(identifer, outlierScoreOfBucket);
                         }
                    }
                    List<Tuple2<String, Map<String, Double>>> result = new ArrayList<>();
                    result.add(new Tuple2<String, Map<String,Double>>(t._1, outlierScoreMap));
                    return result.iterator();
               }
          }).collect();
          
          
          List<Tuple2<String,Double>> finalOutlierScoreList = new ArrayList<>();
          for (Tuple2<String, Map<String, Double>> t1 : outlierScores) {
               for(Entry<String,Double> m : t1._2.entrySet()){
                    finalOutlierScoreList.add(new Tuple2<String, Double>(m.getKey(),m.getValue()));
               }
          }
          
          String identifierColumnName = concernedColumns.stream().filter(x->x.isIdentifier()).map(x->x.getName()).collect(Collectors.toList()).get(0);
          Dataset<Row> histogramOutlierDataset = session.createDataset(finalOutlierScoreList, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE())).withColumnRenamed("_1", identifierColumnName+"_processed").withColumnRenamed("_2", "outlier_score").sort("outlier_score");//.show(100);
          histogramOutlierDataset.show(100,false);
          Dataset<Row> outliers = dataset.join(dataset, dataset.col(identifierColumnName).cast(new StringType()).equalTo(histogramOutlierDataset.col(identifierColumnName+"_processed")));
          outliers.show(100);
          //System.out.println();
          
          
          /*JavaPairRDD<String, String> javaPairRdd = dataset.toJavaRDD().mapToPair(row -> {
               String identifierValue = "";
               String recordValue = "";
               BucketGenerator bucketGenerator = new BucketGenerator();
               for (RecordSchemaVO recordSchemaVO : concernedColumns) {
                    if (recordSchemaVO.isFeature()) {
                         if (recordSchemaVO.isIdentifier()) {
                              identifierValue = String.valueOf(row.get(recordSchemaVO.getPosition()));
                         } else {
                              recordValue = String.valueOf(row.get(recordSchemaVO.getPosition()));
                              bucketGenerator.addBucketKey(bucketGenerator.getBucket(recordValue, recordSchemaVO));
                         }
                    }
               }
               String bucketKeyValue = bucketGenerator.getBucketKey();

               return new Tuple2<String, String>(bucketKeyValue, identifierValue);
          }).groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {

               @Override
               public Tuple2<String, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
                    int frequencyCount = 0;

                    for (Object obj : t._2) {
                         frequencyCount++;
                    }

                    return new Tuple2<>(t._1, String.valueOf(frequencyCount));
               }
          });*/

/*          session.createDataset(JavaPairRDD.toRDD(javaPairRdd), Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                    .toDF("Bucket", "Frequency").show(1000,false);
          ;*/

     }

}
