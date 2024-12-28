package com.flipkart.yak.sep.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.HashMap;
import java.util.Map;

public class SepMetricsPublisher {
  private static final String METRIC_BATCH_TYPE = "batch.";
  private static final String METRIC_EVENT_TYPE = "event.";
  private static final String METRIC_MUTATION_EVENT_TYPE = "mutationEvent.";
  private static final String METRIC_ROW_MUTATION_EVENT_TYPE = "rowMutationEvent.";
  private static final String METRIC_FILTERS_TYPE = "filters.";
  private static final String METRIC_EXCEPTIONS_TYPE = "exceptions.";
  private static final String METRIC_TIMER = "duration";
  private static final String METRIC_INIT = "init";
  private static final String METRIC_COMPLETE = "complete";
  private static final String METRIC_NO_ORIGIN = "noOrigin";
  private static final String METRIC_NO_TOPIC = "noTopic";
  private static final String METRIC_NON_LOCAL = "nonLocal";
  private static final String METRIC_NON_WHITELIST = "nonWhitelist";
  private static final String METRIC_NO_CELLS = "noCells";
  private static final String METRIC_FILTER_ANY = "anyFilter";


  private static final String METRIC_METHOD_REPLICATE_NAME = "replicate.";

  public static final String REPLICATE_BATCH_INIT = "replicateBatchInit";
  public static final String REPLICATE_BATCH_COMPLETE = "replicateBatchComplete";
  public static final String REPLICATE_EVENT_INIT = "replicateEventInit";
  public static final String REPLICATE_MUTATION_EVENT_INIT = "replicateMutationEventInit";
  public static final String REPLICATE_ROW_MUTATION_EVENT_INIT = "replicateRowMutationEventInit";
  public static final String REPLICATE_EVENT_COMPLETE = "replicateEventComplete";
  public static final String REPLICATE_MUTATION_EVENT_COMPLETE = "replicateMutationEventComplete";
  public static final String REPLICATE_ROW_MUTATION_EVENT_COMPLETE = "replicateRowMutationEventComplete";
  public static final String REPLICATE_EXCEPTION = "replicateException";
  public static final String FILTER_NO_ORIGIN = "noOriginFiltering";
  public static final String FILTER_NO_TOPIC = "noTopicFiltering";
  public static final String FILTER_NON_LOCAL = "nonLocalFiltering";
  public static final String FILTER_NON_WHITELIST = "nonWhitelistFiltering";
  public static final String FILTER_NO_CELLS = "noCellsToReplicate";
  public static final String FILTER_ANY = "anyFiltering";

  public static final String REPLICATE_TIMER = "replicateTimer";
  private static final String METRIC_ALL_EXCEPTIONS = "allExceptions";

  private Map<String, String> meterMapping = new HashMap<>();
  private Map<String, String> errorMapping = new HashMap<>();
  private Map<String, String> timerMapping = new HashMap<>();
  private MetricsPublisherImpl publisher;

  public SepMetricsPublisher(MetricRegistry registry, String prefix) {
    publisher = new MetricsPublisherImpl(registry, prefix);

    timerMapping.put(REPLICATE_TIMER, METRIC_METHOD_REPLICATE_NAME + METRIC_TIMER);
    meterMapping.put(REPLICATE_BATCH_INIT, METRIC_METHOD_REPLICATE_NAME + METRIC_BATCH_TYPE + METRIC_INIT);
    meterMapping.put(REPLICATE_BATCH_COMPLETE, METRIC_METHOD_REPLICATE_NAME + METRIC_BATCH_TYPE + METRIC_COMPLETE);
    meterMapping.put(REPLICATE_EVENT_INIT, METRIC_METHOD_REPLICATE_NAME + METRIC_EVENT_TYPE + METRIC_INIT);
    meterMapping.put(REPLICATE_MUTATION_EVENT_INIT, METRIC_METHOD_REPLICATE_NAME + METRIC_MUTATION_EVENT_TYPE + METRIC_INIT);
    meterMapping.put(REPLICATE_ROW_MUTATION_EVENT_INIT, METRIC_METHOD_REPLICATE_NAME + METRIC_ROW_MUTATION_EVENT_TYPE + METRIC_INIT);
    meterMapping.put(REPLICATE_EVENT_COMPLETE, METRIC_METHOD_REPLICATE_NAME + METRIC_EVENT_TYPE + METRIC_COMPLETE);
    meterMapping.put(REPLICATE_MUTATION_EVENT_COMPLETE, METRIC_METHOD_REPLICATE_NAME + METRIC_MUTATION_EVENT_TYPE + METRIC_COMPLETE);
    meterMapping.put(REPLICATE_ROW_MUTATION_EVENT_COMPLETE, METRIC_METHOD_REPLICATE_NAME + METRIC_ROW_MUTATION_EVENT_TYPE + METRIC_COMPLETE);
    meterMapping.put(FILTER_NO_ORIGIN, METRIC_METHOD_REPLICATE_NAME + METRIC_FILTERS_TYPE + METRIC_NO_ORIGIN);
    meterMapping.put(FILTER_NO_TOPIC, METRIC_METHOD_REPLICATE_NAME + METRIC_FILTERS_TYPE + METRIC_NO_TOPIC);
    meterMapping.put(FILTER_NON_LOCAL, METRIC_METHOD_REPLICATE_NAME + METRIC_FILTERS_TYPE + METRIC_NON_LOCAL);
    meterMapping.put(FILTER_NON_WHITELIST, METRIC_METHOD_REPLICATE_NAME + METRIC_FILTERS_TYPE + METRIC_NON_WHITELIST);
    meterMapping.put(FILTER_NO_CELLS, METRIC_METHOD_REPLICATE_NAME + METRIC_FILTERS_TYPE + METRIC_NO_CELLS);
    meterMapping.put(FILTER_ANY, METRIC_METHOD_REPLICATE_NAME + METRIC_FILTERS_TYPE + METRIC_FILTER_ANY);

    errorMapping.put(REPLICATE_EXCEPTION, METRIC_METHOD_REPLICATE_NAME + METRIC_EXCEPTIONS_TYPE);

    // Start metric on application start
    for (String key : meterMapping.keySet()) {
      incrementMetric(key, 0L, false);
    }
    for (String key : errorMapping.keySet()) {
      incrementMetric(errorMapping.get(key) + METRIC_ALL_EXCEPTIONS, 0L, true);
    }
    for (String key : timerMapping.keySet()) {
      getTimer(key).close();
    }
  }

  public void incrementMetric(String metricName) {
    this.publisher.markMeter(meterMapping.get(metricName));
  }

  public void incrementMetric(String metricName, long value, boolean isMetricName) {
    if (isMetricName) {
      this.publisher.markMeter(metricName, value);
    } else {
      this.publisher.markMeter(meterMapping.get(metricName), value);
    }
  }

  public void incrementMetric(String metricName, boolean flag) {
    this.publisher.markMeter(meterMapping.get(metricName), flag);
  }

  public Timer.Context getTimer(String metricName) {
    return this.publisher.getTimer(timerMapping.get(metricName)).time();
  }

  public void incrementErrorMetric(String metricName, Throwable error) {
    this.publisher.markMeter(errorMapping.get(metricName) + METRIC_ALL_EXCEPTIONS);
    this.publisher.markMeter(errorMapping.get(metricName) + error.getClass().getSimpleName());
  }

  public void incrementErrorMetric(String metricName, String errorName) {
    this.publisher.markMeter(errorMapping.get(metricName) + METRIC_ALL_EXCEPTIONS);
    this.publisher.markMeter(errorMapping.get(metricName) + errorName);
  }
}
