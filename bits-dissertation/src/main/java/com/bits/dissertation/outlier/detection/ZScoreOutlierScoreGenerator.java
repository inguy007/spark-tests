package com.bits.dissertation.outlier.detection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StringType;

import com.bits.dissertation.outlier.schema.RecordSchemaVO;

import scala.Tuple2;

public class ZScoreOutlierScoreGenerator implements OutlierScoreGenerator {

     /**
      * 
      */
     private static final long serialVersionUID = -3722707975975903887L;

     /*public void generateOutlierScores2(SparkSession session, Dataset<Row> dataset,
               List<RecordSchemaVO> concernedColumns, Column[] categoricalColumns, Column[] featureColumns) {
          System.out.println("Computing Z-Score outlier scores");
          Dataset<Row> groupedByCategoricalColumn = dataset.groupBy(categoricalColumns)
                    .agg(org.apache.spark.sql.functions.collect_list(featureColumns[0]).alias("sorted_feature"));
     
          groupedByCategoricalColumn.show();
          session.udf().register("computeMedian", new UDF1<Seq<Double>, Double>() {
     
               @Override
               public Double call(Seq<Double> columnData) throws Exception {
                    List<Double> columnDataList = new ArrayList<Double>();
     
                    columnDataList.addAll(scala.collection.JavaConversions.seqAsJavaList(columnData));
                    // List<>
                    Collections.sort(columnDataList);
                    int size = columnDataList.size();
                    // System.out.println("SIZE :"+size);
                    if (size > 1) {
                         int mid = columnDataList.size() / 2;
                         System.out.println("");
                         if (mid % 2 == 0) {
                              return (columnDataList.get(mid) + columnDataList.get(mid - 1)) / 2;
                         } else {
                              return columnDataList.get(mid);
                         }
                    }
                    return columnDataList.get(0);
     
               }
          }, DataTypes.DoubleType);
     
          session.udf().register("computeMedianDeviation", new UDF2<Seq<Double>, Double, Double[]>() {
     
               @Override
               public Double[] call(Seq<Double> columnData, Double median) throws Exception {
                    List<Double> columnDataList = new ArrayList<Double>();
                    columnDataList.addAll(scala.collection.JavaConversions.seqAsJavaList(columnData));
                    Double[] medianDeviationArray = new Double[columnDataList.size()];
                    for (int i = 0; i < columnDataList.size(); i++) {
                         medianDeviationArray[i] = Math.abs(columnDataList.get(i) - median);
                    }
                    return medianDeviationArray;
               }
          }, DataTypes.createArrayType(DataTypes.DoubleType));
     
          session.udf().register("computeMedianAbsoluteDeviation", new UDF1<Seq<Double>, Double>() {
     
               @Override
               public Double call(Seq<Double> columnData) throws Exception {
                    List<Double> columnDataList = new ArrayList<Double>();
                    columnDataList.addAll(scala.collection.JavaConversions.seqAsJavaList(columnData));
                    Collections.sort(columnDataList);
                    int size = columnDataList.size();
                    if (size > 1) {
                         int mid = columnDataList.size() / 2;
                         System.out.println("");
                         if (mid % 2 == 0) {
                              return ((columnDataList.get(mid) + columnDataList.get(mid - 1)) / 2) * 1.4296;
                         } else {
                              return (columnDataList.get(mid)) * 1.4296;
                         }
                    }
                    return (columnDataList.get(0)) * 1.4296;
     
               }
          }, DataTypes.DoubleType);
     
          session.udf().register("computeRobustZScore", new UDF2<Seq<Double>, Double, Double[]>() {
     
               @Override
               public Double[] call(Seq<Double> medianDeviation, Double mad) throws Exception {
                    List<Double> columnDataList = new ArrayList<Double>();
                    columnDataList.addAll(scala.collection.JavaConversions.seqAsJavaList(medianDeviation));
                    Double[] medianDeviationArray = new Double[columnDataList.size()];
                    for (int i = 0; i < columnDataList.size(); i++) {
                         medianDeviationArray[i] = (columnDataList.get(i)) / mad;// Math.abs(columnDataList.get(i)-median);
                    }
                    return medianDeviationArray;
               }
          }, DataTypes.createArrayType(DataTypes.DoubleType));
     
          String[] columnNames = groupedByCategoricalColumn.columns();
          Column[] columns = new Column[columnNames.length + 1];
          for (int i = 0; i < columnNames.length; i++) {
               columns[i] = new Column(columnNames[i]);
          }
     
          columns[columnNames.length] =
                    callUDF("computeMedian", groupedByCategoricalColumn.col("sorted_feature")).alias("Median");
     
          // Dataset<Row> withMedian = groupedByCategoricalColumn.select(groupedByCategoricalColumn.col("Agency
          // Name"),groupedByCategoricalColumn.col("sorted_feature"),
          // callUDF("computeMedian", groupedByCategoricalColumn.col("sorted_feature")).alias("Median"));
     
          Dataset<Row> withMedian = groupedByCategoricalColumn.select(columns);
          withMedian.show(10);
     
          Dataset<Row> withMedianDeviation = withMedian.select(withMedian.col("Agency Name"),
                    withMedian.col("sorted_feature"), withMedian.col("Median"),
                    callUDF("computeMedianDeviation", withMedian.col("sorted_feature"), withMedian.col("Median"))
                              .alias("Median Deviation"));
          withMedianDeviation.show(10);
     
          Dataset<Row> withMedianAbsoluteDeviation = withMedianDeviation.select(withMedianDeviation.col("Agency Name"),
                    withMedianDeviation.col("sorted_feature"),
                    withMedianDeviation.col("Median"),
                    withMedianDeviation.col("Median Deviation"),
                    callUDF("computeMedianAbsoluteDeviation", withMedianDeviation.col("Median Deviation"))
                              .alias("Median Absolute Deviation"));
          withMedianAbsoluteDeviation.show(10);
     
          Dataset<Row> withRobustZScore =
                    withMedianAbsoluteDeviation.select(withMedianAbsoluteDeviation.col("Agency Name"),
                              withMedianAbsoluteDeviation.col("sorted_feature"),
                              withMedianAbsoluteDeviation.col("Median"),
                              withMedianAbsoluteDeviation.col("Median Deviation"),
                              withMedianAbsoluteDeviation.col("Median Absolute Deviation"),
                              callUDF("computeRobustZScore", withMedianAbsoluteDeviation.col("Median Deviation"),
                                        withMedianAbsoluteDeviation.col("Median Absolute Deviation"))
                                                  .alias("Robust Z-Score"));
          withRobustZScore.show(10);
     
     }*/

     @Override
     public void generateOutlierScores(SparkSession session, Dataset<Row> dataset,
               List<RecordSchemaVO> concernedColumns, Column[] categoricalColumns, Column[] featureColumns) {
          List<Tuple2<String, Map<String, Double>>> zScores = dataset.toJavaRDD().mapToPair(row -> {
               String key = "";
               String value = "";
               String identifier = "";
               for (int i = 0; i < concernedColumns.size(); i++) {
                    RecordSchemaVO recordSchema = concernedColumns.get(i);
                    if (recordSchema.isCategorical()) {
                         if (StringUtils.isBlank(key)) {
                              key = String.valueOf(row.get(recordSchema.getPosition()));
                         } else {
                              key = "#" + String.valueOf(row.get(recordSchema.getPosition()));
                         }

                    }
                    if (recordSchema.isIdentifier()) {
                         identifier = String.valueOf(row.get(recordSchema.getPosition()));
                    }
                    if (recordSchema.isFeature()) {
                         value = String.valueOf(row.get(recordSchema.getPosition()));
                    }
               }
               return new Tuple2<>(key, identifier + "~" + value);
          }).groupByKey().flatMapToPair(
                    new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, Map<String, Double>>() {

                         /**
                          * 
                          */
                         private static final long serialVersionUID = -2453384184778468223L;

                         
                         @Override
                         public Iterator<Tuple2<String, Map<String, Double>>> call(Tuple2<String, Iterable<String>> t)
                                   throws Exception {
                              System.out.println("Processing for CATEGORY :"+t._1);
                              List<Tuple2<String, Map<String, Double>>> result = new ArrayList<>();
                              Map<String, Double> identifierFeatureValueMap = new HashMap<>();
                              for (String s : t._2) {
                                   String[] identifierFeature = s.split("~");
                                //   System.out.println("identifier-values" + identifierFeature[0] + " and amount="
                                 //            + identifierFeature[1]);
                                   String identifier = identifierFeature[0];
                                   String value = identifierFeature[1];
                                   identifierFeatureValueMap.put(identifier, Double.parseDouble(value));
                              }

                              Collection<Double> valueSet = identifierFeatureValueMap.values();
                              List<Double> valuesList = new ArrayList<>(valueSet);
                              Collections.sort(valuesList);
                             // System.out.println("Value List :" + ArrayUtils.toString(valuesList.toArray()));
                              int totalValues = valuesList.size();
                              double median = 0;
                              if (totalValues > 1) {
                                   int mid = totalValues / 2;
                                   if (mid % 2 == 0) {
                                        median = (double) (valuesList.get(mid - 1) + valuesList.get(mid)) / (double) 2;
                                   } else {
                                        median = valuesList.get(mid);
                                   }
                              }
                             // System.out.println("Identifier Feature Value Map " + identifierFeatureValueMap);
                             // System.out.println("Median :" + median);
                              Map<String, Double> idenfierMedianDeviationMap = new HashMap<>();
                              for (Entry<String, Double> valueEntry : identifierFeatureValueMap.entrySet()) {
                                   idenfierMedianDeviationMap.put(valueEntry.getKey(),
                                             Math.abs(Double.parseDouble(valueEntry.getKey()) - median));
                              }
                              Collection<Double> medianSet = idenfierMedianDeviationMap.values();
                              List<Double> medianList = new ArrayList<>(medianSet);
                              Collections.sort(medianList);
                             // System.out.println("Median Deviation List :" + ArrayUtils.toString(medianList.toArray()));
                              totalValues = medianList.size();
                              double medianOfMedianDeviation = 0;
                              if (totalValues > 1) {
                                   int mid = totalValues / 2;
                                   if (mid % 2 == 0) {
                                        medianOfMedianDeviation =
                                                  (double) (medianList.get(mid - 1) + medianList.get(mid)) / (double) 2;
                                   } else {
                                        medianOfMedianDeviation = medianList.get(mid);
                                   }
                              }

                              double medianAbsoluteDeviation = 1.4296 * medianOfMedianDeviation;

                             // System.out.println("Median Absolute Deviation :" + medianAbsoluteDeviation);
                             // System.out.println("Identifier Median Deviation Map " + idenfierMedianDeviationMap);

                              Map<String, Double> identifierZscoreMap = new HashMap<>();
                              for (Entry<String, Double> medianDeviationValue : idenfierMedianDeviationMap.entrySet()) {
                                   double zScore = (double) medianDeviationValue.getValue()
                                             / (double) medianAbsoluteDeviation;
                                   identifierZscoreMap.put(medianDeviationValue.getKey(), zScore);
                              }
                              result.add(new Tuple2<String, Map<String, Double>>(t._1, identifierZscoreMap));
                              return result.iterator();
                         }
                    }).collect();
          
          List<Tuple2<String,Double>> finalZScoreList = new ArrayList<>();
          for (Tuple2<String, Map<String, Double>> t1 : zScores) {
               for(Entry<String,Double> m : t1._2.entrySet()){
                    finalZScoreList.add(new Tuple2<String, Double>(m.getKey(),m.getValue()));
               }
          }
          
          String identifierColumnName = concernedColumns.stream().filter(x->x.isIdentifier()).map(x->x.getName()).collect(Collectors.toList()).get(0);
          
         // String identifierColumnDataType = concernedColumns.stream().filter(x->x.isIdentifier()).map(x->x.getDataType()).collect(Collectors.toList()).get(0);
          
          Dataset<Row> zScoreDataSet = session.createDataset(finalZScoreList, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE())).withColumnRenamed("_1", identifierColumnName+"_processed").withColumnRenamed("_2", "robust z-score").sort("robust z-score");//.show(100);
          dataset.show(2);
          dataset.printSchema();
          zScoreDataSet.show(2);
          zScoreDataSet.printSchema();
          Dataset<Row> outliers = dataset.join(dataset, dataset.col(identifierColumnName).cast(new StringType()).equalTo(zScoreDataSet.col(identifierColumnName+"_processed")));
          outliers.show(100);
     }

}
