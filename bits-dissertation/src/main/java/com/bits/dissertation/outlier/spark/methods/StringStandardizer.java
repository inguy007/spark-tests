package com.bits.dissertation.outlier.spark.methods;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.uttesh.exude.common.BaseResource;
import com.uttesh.exude.exception.InvalidDataException;
import com.uttesh.exude.stopping.StoppingResource;

public class StringStandardizer implements Serializable {

     /**
      * 
      */
     private static final long serialVersionUID = -7198954445150559670L;

     public String standardizeString(String content) throws InvalidDataException {
          //System.out.println("Content :" + content);
          String[] tokenized = content.toUpperCase().split("[\\s]|,");
          BaseResource baseResource = new StoppingResource();
          List<String> filteredWords = new ArrayList<>();
          for (int i = 0; i < tokenized.length; i++) {
               String word = tokenized[i].toUpperCase();
               if (word.length() > 1) {
                    String stopWords = baseResource
                              .getProperties(String.valueOf(word.charAt(0)) + String.valueOf(word.charAt(1)));
                    //System.out.println("Stop Words To Match: " + stopWords);
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
          Collections.sort(filteredWords);

          StringBuilder sb = new StringBuilder();
          if (CollectionUtils.isNotEmpty(filteredWords)) {
               for (String filteredWord : filteredWords) {
                    sb.append(filteredWord).append(" ");
               }
          }
          //System.out.println("Filtered Words :" + sb.toString().trim());
          return sb.toString().trim();
     }
}
