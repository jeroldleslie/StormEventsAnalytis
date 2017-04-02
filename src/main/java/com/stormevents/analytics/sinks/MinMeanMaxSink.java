package com.stormevents.analytics.sinks;

import org.apache.storm.mongodb.bolt.MongoUpdateBolt;
import org.apache.storm.mongodb.common.SimpleQueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.SimpleMongoUpdateMapper;

public class MinMeanMaxSink extends MongoUpdateBolt {

  public MinMeanMaxSink(String url) {
    super(url, "minMeanMaxMonthly", new SimpleQueryFilterCreator().withField("key"),
        new SimpleMongoUpdateMapper().withFields("key", 
            "injuries_direct_min", 
            "injuries_direct_max",
            "injuries_direct_mean", 
            "injuries_indirect_min", 
            "injuries_indirect_max", 
            "injuries_indirect_mean",
            "deaths_direct_min", 
            "deaths_direct_max",
            "deaths_direct_mean", 
            "deaths_indirect_min", 
            "deaths_indirect_max", 
            "deaths_indirect_mean",
            "property_damage_min", 
            "property_damage_max", 
            "property_damage_mean",
            "crops_damage_min", 
            "crops_damage_max", 
            "crops_damage_mean",
            "magnitude_min", 
            "magnitude_max", 
            "magnitude_mean",
            "year", "month", "date", "event_type"));
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

}
