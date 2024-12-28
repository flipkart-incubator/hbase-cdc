package com.flipkart.yak.sep.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.HashMap;

public class MetricsPublisherImpl implements MetricsPublisher<Counter, Timer> {

  private final MetricRegistry registry;
  private final String prefix;
  private HashMap<String, Meter> meterHashMap = new HashMap<>();
  private HashMap<String, Counter> counterHashMap = new HashMap<>();
  private HashMap<String, Timer> timerHashMap = new HashMap<>();

  public MetricsPublisherImpl(MetricRegistry registry, String prefix) {
    this.registry = registry;
    this.prefix = prefix;
  }

  @Override public void markMeter(String name, boolean flag) {
    if (flag) {
      markMeter(name);
    }
  }

  @Override public void markMeter(final String name) {
    markMeter(name, 1L);
  }

  @Override public void markMeter(final String name, long value) {
    String metricName = prefix + name;
    meterHashMap.putIfAbsent(metricName, registry.meter(metricName));
    meterHashMap.get(metricName).mark(value);
  }

  @Override public Counter getCounter(final String name) {
    String metricName = prefix + name;
    counterHashMap.putIfAbsent(metricName, registry.counter(metricName));
    return counterHashMap.get(metricName);
  }

  @Override public Timer getTimer(final String name) {
    String metricName = prefix + name;
    timerHashMap.putIfAbsent(metricName, registry.timer(metricName));
    return timerHashMap.get(metricName);
  }
}
