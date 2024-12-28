package com.flipkart.yak.sep.metrics;

public interface MetricsPublisher<U, V> {

  public void markMeter(String name);

  public void markMeter(String name, long value);

  public void markMeter(String name, boolean flag);

  public U getCounter(String name);

  public V getTimer(String name);
}
