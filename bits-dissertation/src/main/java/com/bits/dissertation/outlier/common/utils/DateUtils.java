package com.bits.dissertation.outlier.common.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
	public static String[] strDateFormats = new String[] { "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm:ss",
			"MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "yyyy/dd/MM HH:mm:ss",
			"yyyy/MM/dd HH:mm:ss", "dd-MM-yyyy HH:mm:ss", "MM-dd-yyyy HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
			"yyyy-dd-MM HH:mm:ss", "dd-MM-yyyy",
			"dd-MMM-yyyy HH:mm:ss", "dd.MM.yyyy HH:mm:ss", "dd.MM.yyyy", "dd.MMM.yyyy HH:mm:ss",
			"yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "yyyy-MMM-dd HH:mm:ss" };

	public static SimpleDateFormat[] dateFormats = new SimpleDateFormat[] {

			new SimpleDateFormat("MM/dd/yyyy HH:mm:ss"), new SimpleDateFormat("dd-MM-yyyy"),
			new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss"), new SimpleDateFormat("dd.MM.yyyy HH:mm:ss"),
			new SimpleDateFormat("dd.MM.yyyy"), new SimpleDateFormat("dd.MMM.yyyy HH:mm:ss"),
			new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
			new SimpleDateFormat("yyyy-MM-dd"), new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss"),
			new SimpleDateFormat("M/dd/yyyy"), new SimpleDateFormat("dd.M.yyyy"),
			new SimpleDateFormat("M/dd/yyyy hh:mm:ss a"), new SimpleDateFormat("dd.M.yyyy hh:mm:ss a"),
			new SimpleDateFormat("dd.MMM.yyyy"), new SimpleDateFormat("dd-MMM-yyyy"),
			new SimpleDateFormat("MM-dd-yyyy HH:mm:ss"), new SimpleDateFormat("dd-MM-yyyy HH:mm:ss") };	
	
	public static Date getDateFromString(String input) throws ParseException {
		Date date = null;
		if (null == input) {
			return null;
		}
		for (String strDateFormat : strDateFormats) {
	            try {
	                 SimpleDateFormat format = new SimpleDateFormat(strDateFormat);
	                 format.setLenient(false);
	                 date = format.parse(input);
			} catch (ParseException e) {
				continue;
			}
			if (date != null) {
				break;
			}
		}
		if (date == null) {
			throw new ParseException("Error parsing date from given string ::" + input, 1);
		}
		return date;
	}
	
	public static Date getDateInFormat(Date inputDate) throws ParseException {
          DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
          String dateStr = dateFormat.format(inputDate);
          return dateFormat.parse(dateStr);
     }
	
	public static String getDateStringInFormat(Date inputDate) throws ParseException {
          DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
          return dateFormat.format(inputDate);
     }
	
	public static int getHoursFromDateString(String input) throws ParseException{
	     Date date = getDateFromString(input);
	     Calendar cal = Calendar.getInstance();
	     cal.setTime(date);  
	     int hours = cal.get(Calendar.HOUR_OF_DAY);
	     return hours;
	}
}
