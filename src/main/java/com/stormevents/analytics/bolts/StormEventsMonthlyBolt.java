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

public class StormEventsMonthlyBolt extends BaseRichBolt {
  /**
   * 
   */
  private static final long   serialVersionUID    = 1L;
  private static final Logger LOG                 = Logger.getLogger(StormEventsMonthlyBolt.class);
  private Map<String, Double> deathsDirectMap     = Maps.newHashMap();
  private Map<String, Double> deathsInDirectMap   = Maps.newHashMap();
  private Map<String, Double> injuriesDirectMap   = Maps.newHashMap();
  private Map<String, Double> injuriesInDirectMap = Maps.newHashMap();

  Map<String, Values>         outputMap           = Maps.newConcurrentMap();
  Calendar                    calendar            = Calendar.getInstance();

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("key", 
        "deaths_direct", 
        "deaths_indirect",
        "injuries_direct",
        "injuries_indirect",
        "year", "month", "date", "event_type"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public void cleanup() {
  }

  @Override
  public void execute(Tuple input) {
    String deathsDirect = input.getStringByField(CsvParserUtil.StormDetailsFields.DEATHS_DIRECT.toString());
    String deathsInDirect = input.getStringByField(CsvParserUtil.StormDetailsFields.DEATHS_INDIRECT.toString());

    String injuriesDirect = input.getStringByField(CsvParserUtil.StormDetailsFields.INJURIES_DIRECT.toString());
    String injuriesInDirect = input.getStringByField(CsvParserUtil.StormDetailsFields.INJURIES_INDIRECT.toString());

    int year = Integer.parseInt(input.getStringByField(CsvParserUtil.StormDetailsFields.YEAR.toString()));
    String month = input.getStringByField(CsvParserUtil.StormDetailsFields.MONTH_NAME.toString());
    String event_type = input.getStringByField(CsvParserUtil.StormDetailsFields.EVENT_TYPE.toString());
    String beginYearMonth = input.getStringByField(CsvParserUtil.StormDetailsFields.BEGIN_YEARMONTH.toString());

    calendar.set(year, CsvParserUtil.getMonthInteger(month), 15, 0, 0, 0);

    String key = event_type + "-" + beginYearMonth;

    Double deaths_direct = 0.0;
    if (!deathsDirectMap.containsKey(key)) {
      deathsDirectMap.put(key, 0.0);
    }
    if (!deathsDirect.trim().equals("")) {
      if (!deathsDirect.trim().equals("")) {
        deaths_direct = deathsDirectMap.get(key) + Double.parseDouble(deathsDirect);

        deathsDirectMap.put(key, deaths_direct);
      }
    }

    Double deaths_indirect = 0.0;
    if (!deathsInDirectMap.containsKey(key)) {
      deathsInDirectMap.put(key, 0.0);
    }
    if (!deathsInDirect.trim().equals("")) {
      deaths_indirect = deathsInDirectMap.get(key) + Double.parseDouble(deathsInDirect);
      deathsInDirectMap.put(key, deaths_indirect);
    }

    Double injuries_direct = 0.0;
    if (!injuriesDirectMap.containsKey(key)) {
      injuriesDirectMap.put(key, 0.0);
    }
    if (!injuriesDirect.trim().equals("")) {
      if (!injuriesDirect.trim().equals("")) {
        injuries_direct = injuriesDirectMap.get(key) + Double.parseDouble(injuriesDirect);

        injuriesDirectMap.put(key, injuries_direct);
      }
    }

    Double injuries_indirect = 0.0;
    if (!injuriesInDirectMap.containsKey(key)) {
      injuriesInDirectMap.put(key, 0.0);
    }
    if (!injuriesInDirect.trim().equals("")) {
      injuries_indirect = injuriesInDirectMap.get(key) + Double.parseDouble(injuriesInDirect);
      injuriesInDirectMap.put(key, injuries_indirect);
    }

    outputMap.put(key, new Values(key, deaths_direct, deaths_indirect, injuries_direct, injuries_indirect, year, month,
        calendar.getTime(), event_type));
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
        LOG.info("emiting storm events monthly to sink... ");
        outputMap.forEach((k, v) -> {
          this.collector.emit(v);
        });
      }
    }
  }

}
