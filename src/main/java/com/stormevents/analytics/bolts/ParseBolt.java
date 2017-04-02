package com.stormevents.analytics.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import com.stormevents.analytics.utils.CsvParserUtil;

public class ParseBolt extends BaseRichBolt {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private OutputCollector collector;

  @Override
  public void execute(Tuple input) {
    String inputLine = input.getString(0);
    if(!inputLine.contains("EVENT_ID")){
      collector.emit(CsvParserUtil.getDetailsValues(input.getString(0)));
    }
    collector.ack(input);  
  }

  @Override
  public void prepare(Map arg0, TopologyContext topologyContext, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(CsvParserUtil.getDetailsHeader()));
  }

}
