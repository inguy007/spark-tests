package com.bits.dissertation.outlier.schema;

import java.io.Serializable;

public class RecordSchemaVO implements Serializable{

	/**
      * 
      */
     private static final long serialVersionUID = 218376394654088872L;

     private int position;
	
	private boolean identifier;
	
	private String name;
	private String dataType;
	private boolean categorical;
	private boolean state;
	private boolean feature;
	
	private float similarityTolerance;

	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public boolean isCategorical() {
		return categorical;
	}

	public void setCategorical(boolean categorical) {
		this.categorical = categorical;
	}

	public boolean isState() {
		return state;
	}

	public void setState(boolean state) {
		this.state = state;
	}

     
     public boolean isFeature() {
          return feature;
     }

     
     public void setFeature(boolean feature) {
          this.feature = feature;
     }

     
     public boolean isIdentifier() {
          return identifier;
     }

     
     public void setIdentifier(boolean identifier) {
          this.identifier = identifier;
     }

     
     public float getSimilarityTolerance() {
          return similarityTolerance;
     }

     
     public void setSimilarityTolerance(float similarityTolerance) {
          this.similarityTolerance = similarityTolerance;
     }

     @Override
     public String toString() {
          return "RecordSchemaVO [position=" + position + ", identifier=" + identifier + ", name=" + name
                    + ", dataType=" + dataType + ", categorical=" + categorical + ", state=" + state + ", feature="
                    + feature + ", similarityTolerance=" + similarityTolerance + "]";
     }
}
