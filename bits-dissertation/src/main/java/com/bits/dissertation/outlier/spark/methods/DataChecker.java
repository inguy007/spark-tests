package com.bits.dissertation.outlier.spark.methods;

import java.io.Serializable;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;

public class DataChecker implements ForeachFunction<Row>,Serializable{

     @Override
     public void call(Row t) throws Exception {
          System.out.println(t.schema());
     }

     
     
}
