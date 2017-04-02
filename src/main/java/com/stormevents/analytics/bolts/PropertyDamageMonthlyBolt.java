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

public class PropertyDamageMonthlyBolt extends BaseRichBolt {
  /**
   * 
   */
  private static final long   serialVersionUID = 1L;
  private static final Logger LOG              = Logger.getLogger(PropertyDamageMonthlyBolt.class);
  private Map<String, Double> damages          = Maps.newHashMap();
  Map<String, Values>         outputMap        = Maps.newConcurrentMap();
  Calendar                    calendar         = Calendar.getInstance();

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("key", "property_damage", "year", "month", "date", "event_type"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public void cleanup() {
    // LOG.info("Cleaning up
    // DamageAnalyticsBolt>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
  }

  @Override
  public void execute(Tuple input) {
    String propertyDamage = input.getStringByField(CsvParserUtil.StormDetailsFields.DAMAGE_PROPERTY.toString());
    int year = Integer.parseInt(input.getStringByField(CsvParserUtil.StormDetailsFields.YEAR.toString()));
    String month = input.getStringByField(CsvParserUtil.StormDetailsFields.MONTH_NAME.toString());
    String event_type = input.getStringByField(CsvParserUtil.StormDetailsFields.EVENT_TYPE.toString());
    String beginYearMonth = input.getStringByField(CsvParserUtil.StormDetailsFields.BEGIN_YEARMONTH.toString());

    calendar.set(year, CsvParserUtil.getMonthInteger(month), 15, 0, 0, 0);

    String key = event_type + "-" + beginYearMonth;

    // LOG.info(key);
    Double property_damage = 0.0;
    String regex = "((?<=[a-zA-Z])(?=[0-9]))|((?<=[0-9])(?=[a-zA-Z]))";
    if (!propertyDamage.trim().equals("")) {
      if (!propertyDamage.trim().equals("0")) {
        String property_damages[] = propertyDamage.split(regex);
        if (damages.containsKey(key)) {
          property_damage = damages.get(key) + CsvParserUtil.getWholeDouble(property_damages);
        }
        damages.put(key, property_damage);
        outputMap.put(key, new Values(key, property_damage, year, month, calendar.getTime(), event_type));
      }
    }
  }

  @Override
  public void prepare(Map map, TopologyContext arg1, OutputCollector collector) {
    Timer timer = new Timer();
    long delay = 0;
    long intevalPeriod = Integer.parseInt(map.get("sinkdelay")+"") * 1000;
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
        LOG.info("emmiting property damage  to sink... ");
        outputMap.forEach((k, v) -> {
          this.collector.emit(v);
        });
      }
    }
  }

}
