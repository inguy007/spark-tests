package com.bits.dissertation.outlier.common.utils;

import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;

public class DataTypeUtil {

	public enum Datatype {
		DOUBLE, BIGINT, DATE, TIMESTAMP, BOOLEAN, STRING, NULL
	}
	
	public static DataType getSparkDataType(String dataType){
		if(dataType.equals(Datatype.DOUBLE.name())){
			return DataTypes.DoubleType;
		}else if(dataType.equals(Datatype.BIGINT.name())){
			return DataTypes.IntegerType;
		}else if(dataType.equals(Datatype.DATE.name())){
			return DataTypes.DateType;
		}else if(dataType.equals(Datatype.BOOLEAN.name())){
			return DataTypes.BooleanType;
		}else if(dataType.equals(Datatype.STRING.name())){
			return DataTypes.StringType;
		}else{
			return DataTypes.StringType;
		}
		
	}
	
}
