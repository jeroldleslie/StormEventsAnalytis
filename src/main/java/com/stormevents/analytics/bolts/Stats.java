package com.stormevents.analytics.bolts;

import java.io.Serializable;
import java.math.BigDecimal;

public class Stats implements Serializable {

  private Double  min     = 0.0;
  private Double  max     = 0.0;
  private Double  mean    = 0.0;
  private Double  tot     = 0.0;

  private Integer counter = 0;

  public Double getMin() {
    return min;
  }

  public void setMin(Double value) {
    this.min = Math.min(this.min, value);
  }

  public Double getMax() {
    return max;
  }

  public void setMax(Double value) {
    this.max = Math.max(this.max, value);
  }

  public Double getMean() {
    return mean;
  }

  public void setMean(Double value) {
    tot += value;
    counter++;
    this.mean = tot / counter;
  }

}
