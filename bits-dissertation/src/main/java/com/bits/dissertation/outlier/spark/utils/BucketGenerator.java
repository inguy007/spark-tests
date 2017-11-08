package com.bits.dissertation.outlier.spark.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

import com.bits.dissertation.outlier.common.utils.DataTypeUtil;
import com.bits.dissertation.outlier.schema.RecordSchemaVO;

public class BucketGenerator {

     private Map<Integer,Set<String>> stringRecordBucketMap;
     private StringBuilder bucketKeyBuilder;

     public BucketGenerator() {
          this.stringRecordBucketMap = new HashMap<>();
          this.bucketKeyBuilder = new StringBuilder();
     }

     public String getBucket(String record, RecordSchemaVO recordSchemaVO) {
          String matchingBucket = record;
          if (recordSchemaVO.getDataType().equalsIgnoreCase(DataTypeUtil.Datatype.STRING.name())) {
               Set<String> stringRecordBuckets = stringRecordBucketMap.get(recordSchemaVO.getPosition());
               if (recordSchemaVO.getSimilarityTolerance() > 0) {
                   if (CollectionUtils.isNotEmpty(stringRecordBuckets)) {
                         double highestSimilarityMeasure = 0;
                         boolean similarityFound = false;
                         for (String bucket : stringRecordBuckets) {
                              int currentSimilarityMeasure = SimilarityMeasureUtils.findStringDistance(record, bucket);
                              if (currentSimilarityMeasure > recordSchemaVO.getSimilarityTolerance()) {
                                   if (currentSimilarityMeasure > highestSimilarityMeasure) {
                                        matchingBucket = bucket;
                                        highestSimilarityMeasure = currentSimilarityMeasure;
                                        similarityFound = true;
                                   }
                              }
                         }
                         if(!similarityFound){
                              stringRecordBucketMap.get(recordSchemaVO.getPosition()).add(record);
                         }
                         
                    }else{
                         stringRecordBuckets = new HashSet<>();
                         stringRecordBuckets.add(record);
                         stringRecordBucketMap.put(recordSchemaVO.getPosition(), stringRecordBuckets);
                    }
               }
          }
          return matchingBucket;
     }
     
     public void addBucketKey(String key){
          bucketKeyBuilder.append(key).append("~");
     }
     
     public String getBucketKey(){
          return bucketKeyBuilder.toString();
     }
     
     public void reset(){
          this.stringRecordBucketMap.clear();
     }

}
