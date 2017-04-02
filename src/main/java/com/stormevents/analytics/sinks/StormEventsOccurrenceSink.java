package com.stormevents.analytics.sinks;

import org.apache.storm.mongodb.bolt.MongoUpdateBolt;
import org.apache.storm.mongodb.common.SimpleQueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.SimpleMongoUpdateMapper;

public class StormEventsOccurrenceSink extends MongoUpdateBolt {

  public StormEventsOccurrenceSink(String url) {
    super(
        url, 
        "stormEventOccuranceMonthly", 
        new SimpleQueryFilterCreator().withField("key"),
        new SimpleMongoUpdateMapper().withFields("key", "num_occ", "year", "month", "date", "event_type"));
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

}
