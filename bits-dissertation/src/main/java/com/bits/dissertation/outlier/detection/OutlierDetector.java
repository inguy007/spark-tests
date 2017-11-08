package com.bits.dissertation.outlier.detection;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.bits.dissertation.outlier.common.utils.DataTypeUtil;
import com.bits.dissertation.outlier.common.utils.DateUtils;
import com.bits.dissertation.outlier.schema.FileMetadataVO;
import com.bits.dissertation.outlier.schema.RecordSchemaVO;
import com.bits.dissertation.outlier.spark.standardizer.DataStandardizer;
import com.bits.dissertation.outlier.spark.standardizer.StandardizerUtils;
import com.bits.dissertation.outlier.spark.utils.RuleParserUtils;
import com.bits.dissertation.outlier.spark.utils.SparkSessionProvider;
import com.fasterxml.jackson.databind.ObjectMapper;


public class OutlierDetector implements Runnable {

     private String dataFilePath;
     private String schemaJson;
     private Map<String, ?> configuration;

     public OutlierDetector(String dataFilePath, String schema, Map<String, ?> configuration) {
          this.dataFilePath = dataFilePath;
          this.schemaJson = schema;
          this.configuration = configuration;
     }

     @Override
     public void run() {
          try {
               System.out.println("Running outlier detector");
               SparkSession sparkSession = SparkSessionProvider.getSparkSession();
               StructType structType = null;
               FileMetadataVO fileMetadata = new ObjectMapper().readValue(schemaJson, FileMetadataVO.class);

               List<RecordSchemaVO> selectFeatureFields = new ArrayList<>();

               StructType selectedFeatureStructType = null;
               if (fileMetadata != null) {
                    if (CollectionUtils.isNotEmpty(fileMetadata.getRecordSchema())) {
                         StructField[] fields = new StructField[fileMetadata.getRecordSchema().size()];
                         for (int i = 0; i < fileMetadata.getRecordSchema().size(); i++) {
                              RecordSchemaVO recordSchema = fileMetadata.getRecordSchema().get(i);
                              String fieldName =
                                        recordSchema.getName().replace(" ", "_").replace("(", "_").replace(")", "_");
                              String dataType = recordSchema.getDataType();
                              fields[i] =
                                        new StructField(fieldName, DataTypeUtil.getSparkDataType(dataType), true,
                                                  Metadata.empty());
                              if (recordSchema.isFeature() || recordSchema.isCategorical()) {
                                   selectFeatureFields.add(recordSchema);
                              }

                         }
                         structType = new StructType(fields);
                    }
               }

               String commaDelimtedDateFormats = ArrayUtils.toString(DateUtils.strDateFormats);
               Dataset<Row> dataSet = sparkSession.read()
                         .option("header", "true").option("inferSchema", "true")
                         .option("sep", fileMetadata.getDelimiter())
                         .option("dateFormat", commaDelimtedDateFormats)// .option("timestampFormat", "yyyy/MM/dd
                                                                        // HH:mm:ss")
                         .csv(dataFilePath);
               // dataSet.show(5, false);
               //System.out.println("INFERED SCHEMA " + dataSet.schema());

               List<String> stringColumnList = new ArrayList<>();
               for (RecordSchemaVO r : selectFeatureFields) {
                    if (r.getDataType().equalsIgnoreCase(DataTypeUtil.Datatype.STRING.name())) {
                         stringColumnList.add(r.getName());
                    }
               }
               System.out.println("Before filtering :"+dataSet.count());
               
               //dataSet.show();
               dataSet.createOrReplaceTempView("source_data");
               
               String condition = RuleParserUtils.generateCondition(fileMetadata.getStaticRules());
               String query = "select * from source_data";
               if(StringUtils.isNotBlank(condition)){
                    query = query+" where "+condition;
               }
               System.out.println("Query :"+query);
               Dataset<Row> filteredDataSet = sparkSession.sql(query);
               System.out.println("After filtering :"+filteredDataSet.count());
             
               String commaSeparatedStringColumns = StringUtils.join(stringColumnList, ",");
               Dataset<Row> standardizedDataSet = filteredDataSet.map(row -> {
                    Object[] values = new Object[fileMetadata.getRecordSchema().size()];
                    for (int i = 0; i < fileMetadata.getRecordSchema().size(); i++) {
                         RecordSchemaVO recordSchema = fileMetadata.getRecordSchema().get(i);
                         if (recordSchema.isFeature() || recordSchema.isCategorical()) {
                              String dataType = recordSchema.getDataType();
                              DataStandardizer dataStandardizer = StandardizerUtils.getDataStandardizer(dataType);
                              if (dataStandardizer != null) {
                                   values[i] = dataStandardizer.standardize(row.getString(recordSchema.getPosition()));
                              } else {
                                   values[i] = row.get(i);
                              }
                         } else {
                              values[i] = row.get(i);
                         }
                    }
                    return RowFactory.create(values);
               }, RowEncoder.apply(dataSet.schema()));

               System.out.println("Num partition before repartition :="+standardizedDataSet.rdd().getNumPartitions());
               
               List<String> categoricalFieldNames = fileMetadata.getRecordSchema().stream().filter(x->x.isCategorical()).map(x->x.getName()).collect(Collectors.toList());
               
               List<RecordSchemaVO> featureFields = fileMetadata.getRecordSchema().stream().filter(x->x.isFeature() || x.isIdentifier() || x.isCategorical()).collect(Collectors.toList());
               
               
               if(CollectionUtils.isNotEmpty(categoricalFieldNames)){
                    System.out.println("Categorical Columns :="+ArrayUtils.toString(categoricalFieldNames.toArray()));
                    Column[] categoricalColumns = new Column[categoricalFieldNames.size()];
                    for(int i=0;i<categoricalFieldNames.size();i++){
                         categoricalColumns[i] = standardizedDataSet.col(categoricalFieldNames.get(i));
                    }
                    
                    Column[] featureColumns = new Column[featureFields.size()];
                    for(int i=0;i<featureFields.size();i++){
                         featureColumns[i] = standardizedDataSet.col(featureFields.get(i).getName());
                    }
                    
                   
                    Dataset<Row> categoryPartitionedDF = standardizedDataSet.repartition(categoricalColumns);
                    System.out.println("Num partition after repartition :="+categoryPartitionedDF.rdd().getNumPartitions());
                    
                    OutlierScoreGenerator outlierScoreGenerator = OutlierGeneratorFactory.getOutlierScoreGenerator(featureFields,fileMetadata.isTimeSeries());
                    if(outlierScoreGenerator != null){
                         outlierScoreGenerator.generateOutlierScores(sparkSession,categoryPartitionedDF, featureFields, categoricalColumns, featureColumns);
                    }
               }
          } catch (Exception e) {
               e.printStackTrace();
               System.err.println("Error while reading file path using spark");
               System.exit(0);
          }
     }

     public static void main(String[] args) throws IOException {
          FileMetadataVO fileMetadata = new ObjectMapper().readValue(new File("D:/projects/time-series.json"), FileMetadataVO.class);
          String schema = new ObjectMapper().writeValueAsString(fileMetadata);
          Runnable r = new OutlierDetector("file:///D:/projects/time-series2.csv", schema, null);
          Thread t = new Thread(r);
          t.start();
     }
}
