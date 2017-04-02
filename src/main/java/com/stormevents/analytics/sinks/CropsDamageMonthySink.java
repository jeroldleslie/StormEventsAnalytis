package com.stormevents.analytics.sinks;

import org.apache.storm.mongodb.bolt.MongoUpdateBolt;
import org.apache.storm.mongodb.common.SimpleQueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.SimpleMongoUpdateMapper;

public class CropsDamageMonthySink extends MongoUpdateBolt {

  public CropsDamageMonthySink(String url) {
    super(
        url, 
        "cropsDamageMonthly", 
        new SimpleQueryFilterCreator().withField("key"),
        new SimpleMongoUpdateMapper().withFields("key", "crops_damage", "year", "month", "date", "event_type"));
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

}
