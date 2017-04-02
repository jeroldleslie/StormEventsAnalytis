package com.stormevents.analytics;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.Maps;
import com.stormevents.analytics.bolts.CropsDamageMonthlyBolt;
import com.stormevents.analytics.bolts.LocationsBolt;
import com.stormevents.analytics.bolts.MinMeanMaxBolt;
import com.stormevents.analytics.bolts.ParseBolt;
import com.stormevents.analytics.bolts.PropertyDamageMonthlyBolt;
import com.stormevents.analytics.bolts.StormEventsMonthlyBolt;
import com.stormevents.analytics.bolts.StormEventsOccurrenceBolt;
import com.stormevents.analytics.sinks.CropsDamageMonthySink;
import com.stormevents.analytics.sinks.LocationSink;
import com.stormevents.analytics.sinks.MinMeanMaxSink;
import com.stormevents.analytics.sinks.PropertyDamageMonthySink;
import com.stormevents.analytics.sinks.StormEventsMonthlySink;
import com.stormevents.analytics.sinks.StormEventsOccurrenceSink;

public class StormEventAnalytics {
  private static final Logger LOG         = Logger.getLogger(StormEventAnalytics.class);
  private static String MONGODB_URL = "mongodb://127.0.0.1:27017/storm-events";

  public static void main(String[] args) {
    LOG.info("StormEvent Analytics started");

    if (args.length < 5) {
      LOG.fatal("Incorrect number of arguments. Required arguments: <zk-hosts> <kafka-topic> <zk-path> <cluster-mode: local or prod> <mongodburl ex:mongodb://127.0.0.1:27017/storm-events>");
      System.exit(1);
    }

    // Build Spout configuration using input command line parameters
    final BrokerHosts zkrHosts = new ZkHosts(args[0]);
    final String kafkaTopic = args[1];
    final String zkRoot = args[2];
    final String stormEventsClientId = "storm-events-consumer";
    
    String mode = args[3];
    
    MONGODB_URL = args[4];
    
    final SpoutConfig stormEventsKafkaConf = new SpoutConfig(zkrHosts, kafkaTopic, zkRoot, stormEventsClientId);
    stormEventsKafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

   
    // Build topology to consume message from kafka
    final TopologyBuilder topologyBuilder = new TopologyBuilder();
    // Create KafkaSpout instance using Kafka configuration and add it to
    // topology

    // kafka spout
    topologyBuilder.setSpout("kafka-spout", new KafkaSpout(stormEventsKafkaConf), 1);

    // bolt to parse csv
    topologyBuilder.setBolt("csv-parser", new ParseBolt(), 6).shuffleGrouping("kafka-spout");

    topologyBuilder.setBolt("storm-events", new StormEventsMonthlyBolt()).shuffleGrouping("csv-parser");
    topologyBuilder.setBolt("property-damage", new PropertyDamageMonthlyBolt()).shuffleGrouping("csv-parser");
    topologyBuilder.setBolt("crops-damage", new CropsDamageMonthlyBolt()).shuffleGrouping("csv-parser");
    topologyBuilder.setBolt("stats", new MinMeanMaxBolt()).shuffleGrouping("csv-parser");
    topologyBuilder.setBolt("occurrence", new StormEventsOccurrenceBolt()).shuffleGrouping("csv-parser");
    topologyBuilder.setBolt("location", new LocationsBolt()).shuffleGrouping("csv-parser");

    LocationSink locationSink = new LocationSink(MONGODB_URL);
    topologyBuilder.setBolt("location-sink", locationSink).fieldsGrouping("location",
        new Fields("eventid", "lat", "lon", "year", "month", "date", "event_type"));

    StormEventsOccurrenceSink eventsOccuranceSink = new StormEventsOccurrenceSink(MONGODB_URL);
    eventsOccuranceSink.withUpsert(true);
    topologyBuilder.setBolt("occurrence-sink", eventsOccuranceSink).fieldsGrouping("occurrence", new Fields("key"));

    StormEventsMonthlySink stormEventsMonthlySink = new StormEventsMonthlySink(MONGODB_URL);
    stormEventsMonthlySink.withUpsert(true);
    topologyBuilder.setBolt("storm-events-sink", stormEventsMonthlySink).fieldsGrouping("storm-events",
        new Fields("key"));

    PropertyDamageMonthySink propertyDamageMonthySink = new PropertyDamageMonthySink(MONGODB_URL);
    propertyDamageMonthySink.withUpsert(true);
    topologyBuilder.setBolt("property-damage-sink", propertyDamageMonthySink).fieldsGrouping("property-damage",
        new Fields("key"));

    CropsDamageMonthySink cropsDamageMonthySink = new CropsDamageMonthySink(MONGODB_URL);
    cropsDamageMonthySink.withUpsert(true);
    topologyBuilder.setBolt("crops-damage-sink", cropsDamageMonthySink).fieldsGrouping("crops-damage",
        new Fields("key"));

    MinMeanMaxSink meanMaxSink = new MinMeanMaxSink(MONGODB_URL);
    meanMaxSink.withUpsert(true);
    topologyBuilder.setBolt("stats-sink", meanMaxSink).fieldsGrouping("stats", new Fields("key"));

 
    if(mode.equals("local")){
      final LocalCluster localCluster = new LocalCluster();
      Map<String, String> map = Maps.newHashMap();
      map.put("sinkdelay", "10");
      localCluster.submitTopology("storm-events-topology", map, topologyBuilder.createTopology());
    }else{
      Config conf = new Config();
      conf.setNumWorkers(20);
      conf.setMaxSpoutPending(5000);
      conf.put("sinkdelay", "10");
      try {
        StormSubmitter.submitTopology("storm-events-topology", conf, topologyBuilder.createTopology());
      } catch (AlreadyAliveException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InvalidTopologyException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (AuthorizationException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    
    
    
  }
}
