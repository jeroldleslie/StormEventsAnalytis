package com.stormevents.analytics.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.storm.tuple.Values;

import com.google.common.collect.Maps;

public class CsvParserUtil {
  public static enum StormDetailsFields {
    BEGIN_YEARMONTH, BEGIN_DAY, BEGIN_TIME, END_YEARMONTH, END_DAY, END_TIME, EPISODE_ID, EVENT_ID, STATE, STATE_FIPS, YEAR, MONTH_NAME, EVENT_TYPE, CZ_TYPE, CZ_FIPS, CZ_NAME, WFO, BEGIN_DATE_TIME, CZ_TIMEZONE, END_DATE_TIME, INJURIES_DIRECT, INJURIES_INDIRECT, DEATHS_DIRECT, DEATHS_INDIRECT, DAMAGE_PROPERTY, DAMAGE_CROPS, SOURCE, MAGNITUDE, MAGNITUDE_TYPE, FLOOD_CAUSE, CATEGORY, TOR_F_SCALE, TOR_LENGTH, TOR_WIDTH, TOR_OTHER_WFO, TOR_OTHER_CZ_STATE, TOR_OTHER_CZ_FIPS, TOR_OTHER_CZ_NAME, BEGIN_RANGE, BEGIN_AZIMUTH, BEGIN_LOCATION, END_RANGE, END_AZIMUTH, END_LOCATION, BEGIN_LAT, BEGIN_LON, END_LAT, END_LON, EPISODE_NARRATIVE, EVENT_NARRATIVE, DATA_SOURCE
  }

  public static List<String> getDetailsHeader() {
    String headerString = "BEGIN_YEARMONTH,BEGIN_DAY,BEGIN_TIME,END_YEARMONTH,END_DAY,END_TIME,EPISODE_ID,EVENT_ID,STATE,STATE_FIPS,YEAR,MONTH_NAME,EVENT_TYPE,CZ_TYPE,CZ_FIPS,CZ_NAME,WFO,BEGIN_DATE_TIME,CZ_TIMEZONE,END_DATE_TIME,INJURIES_DIRECT,INJURIES_INDIRECT,DEATHS_DIRECT,DEATHS_INDIRECT,DAMAGE_PROPERTY,DAMAGE_CROPS,SOURCE,MAGNITUDE,MAGNITUDE_TYPE,FLOOD_CAUSE,CATEGORY,TOR_F_SCALE,TOR_LENGTH,TOR_WIDTH,TOR_OTHER_WFO,TOR_OTHER_CZ_STATE,TOR_OTHER_CZ_FIPS,TOR_OTHER_CZ_NAME,BEGIN_RANGE,BEGIN_AZIMUTH,BEGIN_LOCATION,END_RANGE,END_AZIMUTH,END_LOCATION,BEGIN_LAT,BEGIN_LON,END_LAT,END_LON,EPISODE_NARRATIVE,EVENT_NARRATIVE,DATA_SOURCE";
    return (Arrays.asList(headerString.split(",")));
  }

  public static Values getDetailsValues(String csvDetailsValues) {
    Values values = new Values();
    String[] tokens = csvDetailsValues.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    for (String s : tokens) {
      values.add(s.replaceAll("\"", ""));
    }
    return values;
  }

  public static Double getWholeDouble(String[] values) {

    try {
      Double result = Double.parseDouble(values[0]);
      if (values[1].equals("K")) {
        result = result * 1000;
      } else if (values[1].equals("M")) {
        result = result * 1000000;
      }
      return result;
    } catch (Exception e) {
      return 0.0;
    }

  }

  public static int getMonthInteger(String month) {
    Map<String, Integer> map = Maps.newHashMap();
    map.put("January", 0);
    map.put("February", 1);
    map.put("March", 2);
    map.put("April", 3);
    map.put("May", 4);
    map.put("June", 5);
    map.put("July", 6);
    map.put("August", 7);
    map.put("September", 8);
    map.put("October", 9);
    map.put("November", 10);
    map.put("December", 11);
    return map.get(month);
  }

}
