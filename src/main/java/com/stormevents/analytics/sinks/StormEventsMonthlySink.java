package com.stormevents.analytics.sinks;

import org.apache.storm.mongodb.bolt.MongoUpdateBolt;
import org.apache.storm.mongodb.common.SimpleQueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.SimpleMongoUpdateMapper;

public class StormEventsMonthlySink extends MongoUpdateBolt {

  public StormEventsMonthlySink(String url) {
    super(
        url, 
        "stormEventsMonthly", 
        new SimpleQueryFilterCreator().withField("key"),
        new SimpleMongoUpdateMapper().withFields("key", 
            "deaths_direct",
            "deaths_indirect",
            "injuries_direct",
            "injuries_indirect",
            "year", "month", "date", "event_type"));
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

}
