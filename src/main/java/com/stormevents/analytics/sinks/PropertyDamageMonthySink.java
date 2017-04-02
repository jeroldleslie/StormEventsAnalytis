package com.stormevents.analytics.sinks;

import org.apache.storm.mongodb.bolt.MongoUpdateBolt;
import org.apache.storm.mongodb.common.SimpleQueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.SimpleMongoUpdateMapper;

public class PropertyDamageMonthySink extends MongoUpdateBolt {

  public PropertyDamageMonthySink(String url) {
    super(
        url, 
        "propertyDamageMonthly", 
        new SimpleQueryFilterCreator().withField("key"),
        new SimpleMongoUpdateMapper().withFields("key", "property_damage", "year", "month", "date", "event_type"));
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

}
