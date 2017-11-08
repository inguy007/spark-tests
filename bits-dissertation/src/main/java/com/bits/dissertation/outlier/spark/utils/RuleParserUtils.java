package com.bits.dissertation.outlier.spark.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

public class RuleParserUtils {

     public static String generateCondition(List<String> rules){
          StringBuilder sb = new StringBuilder();
          if(CollectionUtils.isNotEmpty(rules)){
               int i=0;
               for(String rule : rules){
                    sb.append("(").append(rule).append(")");
                    if(i != (rules.size()-1)){
                         sb.append(" ").append("AND").append(" ");
                    }
                    i++;
               }
          }
          return sb.toString();
     }
     
     
     public static void main(String[] args){
          List<String> rules = new ArrayList<String>();
          rules.add("'Amount' != null AND 'Amount' > 0");
          rules.add("'Transaction Date' != null");
          rules.add("'Posted Date' != null");
          System.out.println(generateCondition(rules));
     }
}
