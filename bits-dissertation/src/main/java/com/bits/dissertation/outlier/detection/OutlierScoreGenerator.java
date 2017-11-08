package com.bits.dissertation.outlier.detection;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.bits.dissertation.outlier.schema.RecordSchemaVO;

public interface OutlierScoreGenerator extends Serializable {

     void generateOutlierScores(SparkSession session,Dataset<Row> dataset,List<RecordSchemaVO> concernedColumns,Column[] categoricalColumns,Column[] featureColumns);
     
}
