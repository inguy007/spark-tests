package com.bits.dissertation.outlier.spark.standardizer;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;

import com.bits.dissertation.outlier.common.utils.DateUtils;

public class DateDataStandardizer implements DataStandardizer<String>, Serializable {

     @Override
     public String standardize(String input) {
          try {
               Date inputDate = DateUtils.getDateFromString(input);
               return DateUtils.getDateStringInFormat(inputDate);
          } catch (ParseException e) {
               throw new IllegalArgumentException(e);
          }
     }

}
