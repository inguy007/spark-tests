package com.bits.dissertation.outlier.spark.standardizer;

public class StandardizerUtils {

     public static DataStandardizer getDataStandardizer(String dataType) {
          switch (dataType) {
               case "STRING" :
                    return new StringDataStandardizer();
               case "DATE" :
                    return new DateDataStandardizer();
               default :
                    return null;
          }
     }
     
}
