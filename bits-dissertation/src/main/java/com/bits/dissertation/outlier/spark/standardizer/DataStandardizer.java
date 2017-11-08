package com.bits.dissertation.outlier.spark.standardizer;

public interface DataStandardizer<T> {

     T standardize(T input);

}
