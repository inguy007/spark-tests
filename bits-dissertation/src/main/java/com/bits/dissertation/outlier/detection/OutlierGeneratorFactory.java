package com.bits.dissertation.outlier.detection;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;

import com.bits.dissertation.outlier.common.utils.DataTypeUtil;
import com.bits.dissertation.outlier.schema.RecordSchemaVO;

public class OutlierGeneratorFactory {
     
     public static OutlierScoreGenerator getOutlierScoreGenerator(List<RecordSchemaVO> featureColumns,boolean isTimeSeries){
          if(CollectionUtils.isNotEmpty(featureColumns)){
               List<RecordSchemaVO> fc = featureColumns.stream().filter(x->x.isFeature()).collect(Collectors.toList());
               if(fc.size() == 1 && fc.get(0).getDataType().equalsIgnoreCase(DataTypeUtil.Datatype.DOUBLE.name())){
                    return new ZScoreOutlierScoreGenerator();
               }else if(isTimeSeries){
                    return new TimeSeriesOutlierScoreGenerator();
               }else{
                    return new HistogramBasedOutlierScoreGenerator();
               }
          }
          return null;
     }

}
