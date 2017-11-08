package com.bits.dissertation.outlier.detection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.bits.dissertation.outlier.common.utils.DataTypeUtil;
import com.bits.dissertation.outlier.common.utils.DateUtils;
import com.bits.dissertation.outlier.schema.RecordSchemaVO;

import static org.apache.spark.sql.functions.*;


import scala.Tuple2;

public class TimeSeriesOutlierScoreGenerator implements OutlierScoreGenerator{

     /**
      * 
      */
     private static final long serialVersionUID = 8955385192821944768L;

     @Override
     public void generateOutlierScores(SparkSession session, Dataset<Row> dataset,
               List<RecordSchemaVO> concernedColumns, Column[] categoricalColumns, Column[] featureColumns) {
          // TODO Auto-generated method stub
          List<Tuple2<String,Double>> timeSeriesOutlierScore = new ArrayList<>();
          List<Tuple2<String, Iterable<String>>> timeSeriesList = dataset.toJavaRDD().mapToPair(row -> {
               
               String timeIdentifier = "";
               String stateValue = "";
               String identifierValue = "";
               String key = "";
               for (int i = 0; i < concernedColumns.size(); i++) {
                    RecordSchemaVO recordSchema = concernedColumns.get(i);
                    if (recordSchema.isCategorical()) {
                         if (StringUtils.isBlank(key)) {
                              key = String.valueOf(row.get(recordSchema.getPosition()));
                         } else {
                              key = "#" + String.valueOf(row.get(recordSchema.getPosition()));
                         }

                    }else if(recordSchema.getDataType().equalsIgnoreCase(DataTypeUtil.Datatype.DATE.name())){
                         String dateTimeValue = String.valueOf(row.get(recordSchema.getPosition()));
                         int hours = DateUtils.getHoursFromDateString(dateTimeValue);
                         timeIdentifier = String.valueOf(hours);
                    }else if (recordSchema.isState()){
                         if (StringUtils.isBlank(stateValue)) {
                              stateValue = String.valueOf(row.get(recordSchema.getPosition()));
                         } else {
                              stateValue = "-" + String.valueOf(row.get(recordSchema.getPosition()));
                         }
                    }else if(recordSchema.isIdentifier()){
                         identifierValue = String.valueOf(row.get(recordSchema.getPosition()));
                    }
               }
               
              return new Tuple2<String,String>(key,timeIdentifier+"="+stateValue+"$"+identifierValue);
          }).groupByKey().collect();
          
          for(int i=0;i<timeSeriesList.size();i++){
               Tuple2<String, Iterable<String>> categoricalTimeSeries = timeSeriesList.get(i);
               System.out.println("Processing for category :"+categoricalTimeSeries._1);
               List<String> timeToStateListForCategory = new ArrayList<>();
               for(String s : categoricalTimeSeries._2){
                    timeToStateListForCategory.add(s);
               }
               
               List<Tuple2<String, Iterable<String>>> x = session.createDataset(timeToStateListForCategory, Encoders.STRING()).toJavaRDD().mapToPair(row -> {
                    String[] val = row.split("=");
                    String timeValue = val[0];
                    return new Tuple2<>(timeValue,val[1]);
               }).groupByKey().collect();
               
               Map<String,List<String>> stateTransitionList = new HashMap<String, List<String>>();
               for(Tuple2<String,Iterable<String>> t : x){
                    System.out.println("State transitions for time window :"+t._1);
                    String previousState = null;
                    String previousIdentifier = null;
                    for(String s1 : t._2){
                         String[] stateAndIdentifier = s1.split("\\$");
                         String currentState = stateAndIdentifier[0];
                         String currentIdentifier = stateAndIdentifier[1];
                         if(previousState == null || previousIdentifier == null){
                             previousState = currentState;
                             previousIdentifier = currentIdentifier;
                         }else{
                              String stateSequence = previousState+">"+currentState;
                              System.out.println("State transition :"+stateSequence);
                              String identifierSequence = previousIdentifier+">"+currentIdentifier;
                              if(stateTransitionList.get(stateSequence) != null){
                                   stateTransitionList.get(stateSequence).add(identifierSequence);
                              }else{
                                   List<String> n = new ArrayList<>();
                                   n.add(identifierSequence);
                                   stateTransitionList.put(stateSequence, n);
                              }
                         }
                    }
               }
               
               if(MapUtils.isNotEmpty(stateTransitionList)){
                    for(Entry<String, List<String>> e : stateTransitionList.entrySet()){
                         String stateSequence = e.getKey();
                         System.out.println("State Sequence :"+stateSequence);
                         List<String> identifierSequence = e.getValue();
                         int stateSequenceFrequency = identifierSequence.size();
                         System.out.println("Sequency Frequency :"+stateSequenceFrequency);
                         for(String m : identifierSequence){
                              double outlierScoreForSequence = (double)1/(double)stateSequenceFrequency;
                              timeSeriesOutlierScore.add(new Tuple2<String, Double>(m, outlierScoreForSequence));
                         }
                    }
               }
               
               String identifierColumnName = concernedColumns.stream().filter(y->y.isIdentifier()).map(y->y.getName()).collect(Collectors.toList()).get(0);

               
               Dataset<Row> timeSeriesOutlierDataset = session.createDataset(timeSeriesOutlierScore, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE())).withColumnRenamed("_2", "outlier_score").withColumnRenamed("_1",identifierColumnName+"_sequence").sort(desc("outlier_score"));
               timeSeriesOutlierDataset.show(3,false);
               
          }
          
     }

}
