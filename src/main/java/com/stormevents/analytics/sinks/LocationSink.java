package com.stormevents.analytics.sinks;

import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;

public class LocationSink extends MongoInsertBolt {

  public LocationSink(String url) {
    super(url, 
        "location", 
        new SimpleMongoMapper()
        .withFields("eventid", "lat", "lon", "year", "month", "date", "event_type"));
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

}
