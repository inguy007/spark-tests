package com.bits.dissertation.outlier.schema;

import java.io.Serializable;
import java.util.List;

public class FileMetadataVO implements Serializable{

     /**
      * 
      */
     private static final long serialVersionUID = -8127712847259593291L;
     private String fileName;
     private String delimiter;
     private List<RecordSchemaVO> recordSchema;
     
     private List<String> staticRules;
     
     private boolean timeSeries;

     public FileMetadataVO(String fileName, String delimiter, List<RecordSchemaVO> recordSchema) {
          this.fileName = fileName;
          this.delimiter = delimiter;
          this.recordSchema = recordSchema;
     }
     
     public FileMetadataVO(){
          
     }

     public String getFileName() {
          return fileName;
     }

     public void setFileName(String fileName) {
          this.fileName = fileName;
     }

     public String getDelimiter() {
          return delimiter;
     }

     public void setDelimiter(String delimiter) {
          this.delimiter = delimiter;
     }

     public List<RecordSchemaVO> getRecordSchema() {
          return recordSchema;
     }

     public void setRecordSchema(List<RecordSchemaVO> recordSchema) {
          this.recordSchema = recordSchema;
     }

     
     public List<String> getStaticRules() {
          return staticRules;
     }

     
     public void setStaticRules(List<String> staticRules) {
          this.staticRules = staticRules;
     }

     
     public boolean isTimeSeries() {
          return timeSeries;
     }

     
     public void setTimeSeries(boolean timeSeries) {
          this.timeSeries = timeSeries;
     }

}
