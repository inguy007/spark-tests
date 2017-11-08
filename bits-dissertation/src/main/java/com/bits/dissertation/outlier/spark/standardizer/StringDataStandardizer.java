package com.bits.dissertation.outlier.spark.standardizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.uttesh.exude.common.BaseResource;
import com.uttesh.exude.stopping.StoppingResource;

public class StringDataStandardizer implements DataStandardizer<String>, Serializable {

     /**
      * 
      */
     private static final long serialVersionUID = 8964108186455918393L;

     @Override
     public String standardize(String input) {
          String[] tokenized = input.toUpperCase().split("[\\s]|,|\\.");
          BaseResource baseResource = new StoppingResource();
          Set<String> filteredWords = new HashSet<>();
          for (int i = 0; i < tokenized.length; i++) {
               String word = tokenized[i].toUpperCase();
               if (word.length() > 1) {
                    String stopWords = baseResource
                              .getProperties(String.valueOf(word.charAt(0)) + String.valueOf(word.charAt(1)));
                  //  System.out.println("Stop Words To Match: " + stopWords);
                    if (StringUtils.isNotBlank(stopWords)) {
                         stopWords = stopWords.toUpperCase();
                         if (!stopWords.contains(word)) {
                              filteredWords.add(word);
                         }
                    } else {
                         filteredWords.add(word);
                    }

               }
          }
          List<String> filteredWordsList = new ArrayList<>(filteredWords);
          Collections.sort(filteredWordsList);

          StringBuilder sb = new StringBuilder();
          if (CollectionUtils.isNotEmpty(filteredWordsList)) {
               for (String filteredWord : filteredWordsList) {
                    sb.append(filteredWord).append(" ");
               }
          }
         // System.out.println("Filtered Words :" + sb.toString().trim());
          return sb.toString().trim();
     }

}
