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

public class MinMeanMaxBolt extends BaseRichBolt {
  /**
   * 
   */
  private static final long   serialVersionUID  = 1L;
  Calendar                    calendar          = Calendar.getInstance();
  Map<String, Stats>          injuriesDirectMap = Maps.newHashMap();
  Map<String, Stats>          injuriesInDirectMap = Maps.newHashMap();
  Map<String, Stats>          deathsInDirectMap = Maps.newHashMap();
  Map<String, Stats>          deathsDirectMap = Maps.newHashMap();
  Map<String, Stats>          propertyDamageMap = Maps.newHashMap();
  Map<String, Stats>          cropsDamageMap = Maps.newHashMap();
  Map<String, Stats>          magnitudeMap = Maps.newHashMap();
  
  Map<String, Values>         outputMap         = Maps.newConcurrentMap();
  private static final Logger LOG               = Logger.getLogger(MinMeanMaxBolt.class);

  @Override
  public void execute(Tuple input) {
    String injuries_direct = input.getStringByField(CsvParserUtil.StormDetailsFields.INJURIES_DIRECT.toString());
    String injuries_indirect = input.getStringByField(CsvParserUtil.StormDetailsFields.INJURIES_INDIRECT.toString());

    String deaths_direct = input.getStringByField(CsvParserUtil.StormDetailsFields.DEATHS_DIRECT.toString());
    String deaths_indirect = input.getStringByField(CsvParserUtil.StormDetailsFields.DEATHS_INDIRECT.toString());

    String property_damage = input.getStringByField(CsvParserUtil.StormDetailsFields.DAMAGE_PROPERTY.toString());
    String crops_damage = input.getStringByField(CsvParserUtil.StormDetailsFields.DAMAGE_CROPS.toString());

    String magnitude = input.getStringByField(CsvParserUtil.StormDetailsFields.MAGNITUDE.toString());

    int year = Integer.parseInt(input.getStringByField(CsvParserUtil.StormDetailsFields.YEAR.toString()));
    String month = input.getStringByField(CsvParserUtil.StormDetailsFields.MONTH_NAME.toString());
    String event_type = input.getStringByField(CsvParserUtil.StormDetailsFields.EVENT_TYPE.toString());
    String beginYearMonth = input.getStringByField(CsvParserUtil.StormDetailsFields.BEGIN_YEARMONTH.toString());

    String key = event_type + "-" + beginYearMonth;
    calendar.set(year, CsvParserUtil.getMonthInteger(month), 15, 0, 0, 0);

    if (!injuries_direct.trim().equals("")) {
      Double injuriesDDouble = Double.parseDouble(injuries_direct);
      if (injuriesDirectMap.get(key) != null) {
        Stats s = injuriesDirectMap.get(key);
        s.setMin(injuriesDDouble);
        s.setMax(injuriesDDouble);
        s.setMean(injuriesDDouble);
      } else {
        Stats s = new Stats();
        s.setMin(injuriesDDouble);
        s.setMax(injuriesDDouble);
        s.setMean(injuriesDDouble);
        injuriesDirectMap.put(key, s);
      }
    }
    
    if (!injuries_indirect.trim().equals("")) {
      Double injuriesIDDouble = Double.parseDouble(injuries_direct);
      if (injuriesInDirectMap.get(key) != null) {
        Stats s = injuriesInDirectMap.get(key);
        s.setMin(injuriesIDDouble);
        s.setMax(injuriesIDDouble);
        s.setMean(injuriesIDDouble);
      } else {
        Stats s = new Stats();
        s.setMin(injuriesIDDouble);
        s.setMax(injuriesIDDouble);
        s.setMean(injuriesIDDouble);
        injuriesInDirectMap.put(key, s);
      }
    }
    
    if (!deaths_direct.trim().equals("")) {
      Double deathsDDouble = Double.parseDouble(deaths_direct);
      if (deathsDirectMap.get(key) != null) {
        Stats s = deathsDirectMap.get(key);
        s.setMin(deathsDDouble);
        s.setMax(deathsDDouble);
        s.setMean(deathsDDouble);
      } else {
        Stats s = new Stats();
        s.setMin(deathsDDouble);
        s.setMax(deathsDDouble);
        s.setMean(deathsDDouble);
        deathsDirectMap.put(key, s);
      }
    }
    
    if (!deaths_indirect.trim().equals("")) {
      Double deathsIDDouble = Double.parseDouble(deaths_direct);
      if (deathsInDirectMap.get(key) != null) {
        Stats s = deathsInDirectMap.get(key);
        s.setMin(deathsIDDouble);
        s.setMax(deathsIDDouble);
        s.setMean(deathsIDDouble);
      } else {
        Stats s = new Stats();
        s.setMin(deathsIDDouble);
        s.setMax(deathsIDDouble);
        s.setMean(deathsIDDouble);
        deathsInDirectMap.put(key, s);
      }
    }
    
    
    if (propertyDamageMap.get(key) == null) {
      Stats s = new Stats();
      s.setMin(0.0);
      s.setMax(0.0);
      s.setMean(0.0);
      propertyDamageMap.put(key, s);
    }
    String regex = "((?<=[a-zA-Z])(?=[0-9]))|((?<=[0-9])(?=[a-zA-Z]))";
    if (!property_damage.trim().equals("")) {
      String property_damages[] = property_damage.split(regex);
      Double propertyDamageDouble = CsvParserUtil.getWholeDouble(property_damages);
      Stats s = propertyDamageMap.get(key);
      s.setMin(propertyDamageDouble);
      s.setMax(propertyDamageDouble);
      s.setMean(propertyDamageDouble);
    }
    
    
    if (cropsDamageMap.get(key) == null) {
      Stats s = new Stats();
      s.setMin(0.0);
      s.setMax(0.0);
      s.setMean(0.0);
      cropsDamageMap.put(key, s);
    }
    if (!crops_damage.trim().equals("")) {
      String crops_damages[] = crops_damage.split(regex);
      Double cropsDamageDouble = CsvParserUtil.getWholeDouble(crops_damages);
      Stats s = cropsDamageMap.get(key);
      s.setMin(cropsDamageDouble);
      s.setMax(cropsDamageDouble);
      s.setMean(cropsDamageDouble);
    }

    
    if (magnitudeMap.get(key) == null) {
      Stats s = new Stats();
      s.setMin(0.0);
      s.setMax(0.0);
      s.setMean(0.0);
      magnitudeMap.put(key, s);
    }
    if (!magnitude.trim().equals("")) {
      Double magnitudeDouble = Double.parseDouble(magnitude);
      Stats s = magnitudeMap.get(key);
      s.setMin(magnitudeDouble);
      s.setMax(magnitudeDouble);
      s.setMean(magnitudeDouble);
    }
    
    outputMap.put(key, new Values(key, 
        injuriesDirectMap.get(key).getMin(), 
        injuriesDirectMap.get(key).getMax(),
        injuriesDirectMap.get(key).getMean(), 
        injuriesInDirectMap.get(key).getMin(), 
        injuriesInDirectMap.get(key).getMax(),
        injuriesInDirectMap.get(key).getMean(),
        deathsDirectMap.get(key).getMin(), 
        deathsDirectMap.get(key).getMax(),
        deathsDirectMap.get(key).getMean(), 
        deathsInDirectMap.get(key).getMin(), 
        deathsInDirectMap.get(key).getMax(),
        deathsInDirectMap.get(key).getMean(),
        propertyDamageMap.get(key).getMin(), 
        propertyDamageMap.get(key).getMax(),
        propertyDamageMap.get(key).getMean(),
        cropsDamageMap.get(key).getMin(), 
        cropsDamageMap.get(key).getMax(),
        cropsDamageMap.get(key).getMean(),
        magnitudeMap.get(key).getMin(), 
        magnitudeMap.get(key).getMax(),
        magnitudeMap.get(key).getMean(),
        year,
        month, calendar.getTime(), event_type));
  }

  @Override
  public void prepare(Map map, TopologyContext arg1, OutputCollector collector) {
    Timer timer = new Timer();
    long delay = 0;
    long intevalPeriod = Integer.parseInt(map.get("sinkdelay")+"") * 1000;
    timer.scheduleAtFixedRate(new CollecterTimer(collector), delay, intevalPeriod);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("key", 
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
        "year",
        "month", "date", "event_type"));
  }

  class CollecterTimer extends TimerTask {
    private final Logger LOG = Logger.getLogger(CollecterTimer.class);
    OutputCollector      collector;

    CollecterTimer(OutputCollector collector) {

      this.collector = collector;
    }

    public void run() {
      synchronized (this) {
        LOG.info("emmiting min max mean to sink... ");
        outputMap.forEach((k, v) -> {
          this.collector.emit(v);
        });
      }
    }
  }
}
