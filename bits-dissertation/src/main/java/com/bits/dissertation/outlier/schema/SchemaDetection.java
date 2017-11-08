package com.bits.dissertation.outlier.schema;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import com.bits.dissertation.outlier.common.utils.DateUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SchemaDetection {
	private static Map<String, Integer> delimiterCounterMap = new HashMap<>();

	private static final Map<Datatype, Integer> datatypePriorityMap = new HashMap<>();
	
	private static final Map<Integer,Datatype> dataTypeMap = new HashMap<>();

	private static String delimiter = "";

	private static int numberOfColumns = 0;

	static {
		datatypePriorityMap.put(Datatype.STRING, 1);
		datatypePriorityMap.put(Datatype.DOUBLE, 2);
		datatypePriorityMap.put(Datatype.BIGINT, 3);
	}

	private enum Datatype {
		DOUBLE, BIGINT, DATE, TIMESTAMP, BOOLEAN, STRING, NULL
	}

	public static void main(String[] args) throws IOException {
		FileMetadataVO fileMetadata = generateSchema("file:///D:/projects/time-series2.csv");
		String schema = new ObjectMapper().writeValueAsString(fileMetadata);
		System.out.println("Schema is : "+schema);
		FileUtils.writeStringToFile(new File("D:/projects/schema"+Math.random()+".json"), schema);
		//System.out.println("Written schema file");
	}

	public static FileMetadataVO generateSchema(String filePath) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		Path path = new Path(filePath);
		Text header = new Text();
		if (!fs.exists(path)) {
			throw new RuntimeException("File path " + path + "not found");
		}
		FileStatus fileStatus = fs.getFileStatus(new Path(filePath));
		long fileSize = fileStatus.getLen();
		try (FSDataInputStream in = fs.open(fileStatus.getPath()); LineReader lineReader = new LineReader(in)) {

			Text line = new Text();
			
			lineReader.readLine(header);
			// Figure out the delimiter.
			while (lineReader.readLine(line) != 0) {
				Map<String, Integer> specialCharacterMap = getSpecialChars(line.toString());
				System.out.println(specialCharacterMap);
				if (MapUtils.isEmpty(delimiterCounterMap)) {
					delimiterCounterMap.putAll(specialCharacterMap);
				}
				realign(delimiterCounterMap, specialCharacterMap);
				if (delimiterCounterMap.size() == 1) {
					break;
				}
			}
			if (delimiterCounterMap.size() == 1) {
				for (String key : delimiterCounterMap.keySet()) {
					delimiter = key;
				}
			}
		}
		
		try (FSDataInputStream in = fs.open(fileStatus.getPath()); LineReader lineReader = new LineReader(in)) {

			Text line = new Text();
			//Text header = new Text();
			lineReader.readLine(header);
			int count = 0;
			int max = 100;
			// Figure out the delimiter.
			while (lineReader.readLine(line) != 0 && (count++ < max)) {
				String[] dataArray = line.toString().split(delimiter);
				for(int i=0;i<dataArray.length;i++){
					Datatype dataType = getDataType(dataArray[i]);
					dataTypeMap.put(i, dataType);
				}
			}
		}
		
		String[] columnNames = header.toString().split(delimiter);
		List<RecordSchemaVO> schema = new ArrayList<>();
		for(int i=0;i<columnNames.length;i++){
			RecordSchemaVO recordSchema = new RecordSchemaVO();
			recordSchema.setPosition(i);
			recordSchema.setName(columnNames[i]);
			recordSchema.setDataType(dataTypeMap.get(i).name());
			schema.add(recordSchema);
		}
		
		FileMetadataVO fileMetadata = new FileMetadataVO(path.getName(), delimiter, schema);
		
		System.out.println(new ObjectMapper().writeValueAsString(fileMetadata));
		return fileMetadata;
	}

	private static Map<String, Integer> getSpecialChars(String line) {
		Map<String, Integer> specialChars = new HashMap<String, Integer>();
		Pattern lettersAndDigits = Pattern.compile("[a-zA-Z0-9 ]");
		StringCharacterIterator iterator = new StringCharacterIterator(line);
		boolean stringBegins = false;
		for (char c = iterator.first(); c != iterator.DONE; c = iterator.next()) {
			String character = String.valueOf(c);
			if (character.equals("\"")) {
				stringBegins = !stringBegins;
				continue;
			}
			if (stringBegins && !character.equals("\"")) {
				continue;
			}
			Matcher matcher = lettersAndDigits.matcher(character);
			if (!matcher.matches()) {

				if (specialChars.get(character) == null) {
					specialChars.put(character, 1);
				} else {

					specialChars.put(character, specialChars.get(character) + 1);
				}
			}
		}
		specialChars.remove(":");
		specialChars.remove("/");
		return specialChars;
	}

	private static void realign(Map<String, Integer> base, Map<String, Integer> run) {
		Map<String, Integer> temp = new HashMap<String, Integer>(base);
		for (Entry<String, Integer> entry : base.entrySet()) {
			if (run.get(entry.getKey()) != null && run.get(entry.getKey()) != entry.getValue()) {
				temp.remove(entry.getKey());
			} else if (run.get(entry.getKey()) == null) {
				temp.remove(entry.getKey());
			}

		}
		base.clear();
		base.putAll(temp);
		if (base.isEmpty()) {
			throw new RuntimeException("Could not determine delimiter.");
		}
	}

	private static Datatype getDataType(String data) {
		// Quoted == STRING
		if (data.startsWith("\"") && data.endsWith("\"")) {
			// Some times date data is also quoted
			data = StringUtils.substringBetween(data, "\"", "\"");
		}

		// NUMBER
		if (data.contains(".")) {
			try {
				if (Double.valueOf(data) != null) {
					return Datatype.DOUBLE;
				}
			} catch (NumberFormatException e) {
				// Quietly ignore any exceptions.
			}
		} else {
			try {
				if (Long.valueOf(data) != null) {
					return Datatype.BIGINT;
				}
			} catch (NumberFormatException e) {
				// Quietly ignore any exceptions.
			}
		}

		// DATE
		try {
			Date date = DateUtils.getDateFromString(data);
			if (date != null) {
				return Datatype.DATE;
			}

		} catch (ParseException e) {
			// TODO Auto-generated catch block
		}

		// Boolean
		if (data.equalsIgnoreCase("true") || data.equalsIgnoreCase("false")) {
			return Datatype.BOOLEAN;
		}

		// String
		return Datatype.STRING;
	}

}
