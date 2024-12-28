package com.flipkart.yak.sep.kafka;

/*
 * Created by Amanraj on 01/02/19 .
 */

import com.flipkart.yak.sep.KafkaReplicationEndPoint;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class KafkaHandler {

  private KafkaHandler() {
  }

  private static final Logger logger = LoggerFactory.getLogger(KafkaReplicationEndPoint.class);

  private static Producer<byte[], byte[]> buildProducer(KafkaReplicationConfig cfConfig, String cf) {
    Map<String, Object> props = cfConfig.getKafkaConfig();
    logger.info("got props {} for conf cf {} ", props, cf);

    if (props == null) {
      return null;
    }
    return new KafkaProducer<>(props);
  }

  public static void stopKafka(Map<String, Producer<byte[], byte[]>> kafkaPublishers)
      throws Exception {
    if (kafkaPublishers == null) {
      return;
    }
    Exception th = null;
    for (java.util.Map.Entry<String, Producer<byte[], byte[]>> entry : kafkaPublishers.entrySet()) {
      try {
        entry.getValue().close();
      } catch (Exception e) {
        logger.error("Failed to close producer {}", entry.getKey(), e);
        th = e;
      }
    }
    if (th != null) throw th;
  }

  public static Map<String, Producer<byte[], byte[]>> buildKafkaPublishers(
      Map<String, KafkaReplicationConfig> cfConfigs) {
    Map<String, Producer<byte[], byte[]>> kafkaPublishers = new ConcurrentHashMap<>();
    if (cfConfigs.isEmpty()) {
      return kafkaPublishers;
    }
    for (Map.Entry<String, KafkaReplicationConfig> cfEntry : cfConfigs.entrySet()) {
      String cf = cfEntry.getKey().trim();
      if (cf.isEmpty()) {
        continue;
      }
      Producer<byte[], byte[]> producer = buildProducer(cfEntry.getValue(), cf);
      if (producer != null) {
        kafkaPublishers.putIfAbsent(cf, producer);
      }
    }
    String configuredBrokers = kafkaPublishers.keySet().stream().collect(Collectors.joining(","));
    logger.info("Configured brokers are : {} ", configuredBrokers);
    return kafkaPublishers;
  }
}
