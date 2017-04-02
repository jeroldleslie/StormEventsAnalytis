package com.stormevents.analytics.bolts;

import java.util.Calendar;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.common.collect.Maps;
import com.stormevents.analytics.utils.CsvParserUtil;

public class LocationsBolt  extends BaseRichBolt {
  /**
   * 
   */
  private static final long   serialVersionUID = 1L;
  private static final Logger LOG              = Logger.getLogger(LocationsBolt.class);
  private Map<String, Double> damages          = Maps.newHashMap();
  Map<String, Values>         outputMap        = Maps.newConcurrentMap();
  Calendar                    calendar         = Calendar.getInstance();

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("eventid", "lat", "lon",  "year", "month", "date", "event_type"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public void cleanup() {
   // LOG.info("Cleaning up DamageAnalyticsBolt>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
  }

  @Override
  public void execute(Tuple input) {
    
    String eventid = input.getStringByField(CsvParserUtil.StormDetailsFields.EVENT_ID.toString());
    int year = Integer.parseInt(input.getStringByField(CsvParserUtil.StormDetailsFields.YEAR.toString()));
    String month = input.getStringByField(CsvParserUtil.StormDetailsFields.MONTH_NAME.toString());
    String event_type = input.getStringByField(CsvParserUtil.StormDetailsFields.EVENT_TYPE.toString());
    String beginYearMonth = input.getStringByField(CsvParserUtil.StormDetailsFields.BEGIN_YEARMONTH.toString());
    
    String lat = input.getStringByField(CsvParserUtil.StormDetailsFields.BEGIN_LAT.toString());
    String lon = input.getStringByField(CsvParserUtil.StormDetailsFields.BEGIN_LON.toString());
    calendar.set(year, CsvParserUtil.getMonthInteger(month), 15, 0, 0, 0);
    
    String key = event_type + "-" + beginYearMonth;
    
    if(!lat.trim().equals("") && !lon.trim().equals("")){
      outputMap.put(key,new Values(eventid, lat, lon, year, month, calendar.getTime(), event_type));
    }
  }

  @Override
  public void prepare(Map map, TopologyContext arg1, OutputCollector collector) {
    Timer timer = new Timer();
    long delay = 0;
    long intevalPeriod = Integer.parseInt(map.get("sinkdelay") + "") * 1000;
    timer.scheduleAtFixedRate(new CollecterTimer(collector), delay, intevalPeriod);
  }
  
  class CollecterTimer extends TimerTask {
    private final Logger LOG = Logger.getLogger(CollecterTimer.class);
    OutputCollector      collector;

    CollecterTimer(OutputCollector collector) {

      this.collector = collector;
    }

    public void run() {
      synchronized (this) {
        LOG.info("emiting location to sink... ");
        outputMap.forEach((k, v) -> {
          this.collector.emit(v);
        });
      }
    }
  }

}
