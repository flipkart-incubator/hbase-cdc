package com.flipkart.yak.sep;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.yak.sep.commons.CBConfig;
import com.flipkart.yak.sep.commons.MessageQueueReplicationEndpoint;
import com.flipkart.yak.sep.commons.SepConfig;
import com.flipkart.yak.sep.commons.exceptions.SepConfigException;
import com.flipkart.yak.sep.commons.exceptions.SepException;
import com.flipkart.yak.sep.kafka.KafkaHandler;
import com.flipkart.yak.sep.kafka.KafkaReplicationConfig;
import com.flipkart.yak.sep.metrics.SepMetricsPublisher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto.SepMessage;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto.SepMessageV2;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;


/**
 * Simple Kafka producer based replication endPoint takes columnFamily = key to kafka producer -
 * (Effectively we isolate kafka producer client per column family) takes columnQualifier = topic in
 * kafka brokers takes row = keyId in kafka key message (partition key) sep.kafka.config.path =
 * /tmp/sep_conf.json
 *
 * @author gokulvanan.v
 */
public class KafkaReplicationEndPoint extends MessageQueueReplicationEndpoint<KafkaReplicationConfig, RecordMetadata> {

  private static final Logger logger = LoggerFactory.getLogger(KafkaReplicationEndPoint.class);
  private Map<String, Producer<byte[], byte[]>> publisherMap;
  private int timeout = 5000; //future.getTimeout
  private Configuration hbaseConfig;
  private static final ObjectMapper mapper = new ObjectMapper();



  private Map<String, KafkaReplicationConfig> getCfConfig() throws IOException {
    String confPath = hbaseConfig.get("sep.kafka.config.path");
    if (confPath != null) {
      SepConfig<KafkaReplicationConfig> conf = mapper.readValue(new File(confPath),  new TypeReference<SepConfig<KafkaReplicationConfig>>() {});
      logger.info("Configuration read {}" , conf.getCfConfig());
      return conf.getCfConfig();
    } else {
      logger.warn("No config.path configured for kafka push");
      return new HashMap<>();
    }
  }

  private boolean getPropagateMutationValue() throws IOException {
    String confPath = hbaseConfig.get("sep.kafka.config.path");
    if (confPath != null) {
      SepConfig<KafkaReplicationConfig> conf = mapper.readValue(new File(confPath),  new TypeReference<SepConfig<KafkaReplicationConfig>>() {});
      logger.debug("Propagate Mutation {}" , conf.getPropogateMutation());
      return conf.getPropogateMutation();
    } else {
      logger.warn("Propagate Mutation could not be defined as no config.path configured");
      return false;
    }
  }

  private CBConfig getCbConfig() throws IOException {
    String confPath = hbaseConfig.get("sep.kafka.config.path");
    if (confPath != null) {
      SepConfig<KafkaReplicationConfig> conf = mapper.readValue(new File(confPath),  new TypeReference<SepConfig<KafkaReplicationConfig>>() {});
      logger.info("Circuit Breaking Configuration read  {}" , conf.getCbConfig());
      return conf.getCbConfig();
    } else {
      logger.warn("No config.path configured for Circuit Breaking");
      return new CBConfig();
    }
  }

  private void updateConfigs(Map<String, KafkaReplicationConfig> cfConfigMap,
                             Map<String, Producer<byte[], byte[]>> publisherMap , boolean propogateMutationFlag, CBConfig cbConfig) {
    this.cbConfig = cbConfig;
    this.propogateMutationFlag = propogateMutationFlag;
    this.publisherMap = publisherMap;
    this.cfConfigMap = cfConfigMap;
    String timeoutKey = "sep.kafka.timeout";
    String timeoutVal = hbaseConfig.get(timeoutKey, String.valueOf(this.timeout));
    this.timeout = Integer.parseInt(timeoutVal);
  }

  public Map<String, Producer<byte[], byte[]>> buildKafkaPublishers(
          Map<String, KafkaReplicationConfig> cfConfigs) {
    return KafkaHandler.buildKafkaPublishers(cfConfigs);
  }

  @Override
  public void reloadConfig() throws SepConfigException {
    Map<String, KafkaReplicationConfig> cfConfigMap = null;
    boolean propogateMutationFlag = false;
    CBConfig cbConfig = new CBConfig();
    try {
      propogateMutationFlag = getPropagateMutationValue();
      cfConfigMap = getCfConfig();
      cbConfig = getCbConfig();
      Map<String, Producer<byte[], byte[]>> kafkaPublishers = buildKafkaPublishers(cfConfigMap);
      updateConfigs(cfConfigMap, kafkaPublishers, propogateMutationFlag, cbConfig);
      circuitBreakerInit();
    } catch (Exception e) {
      throw new SepConfigException(e);
    }
  }

  @Override
  public void stopReplication() throws SepException {
    try {
      KafkaHandler.stopKafka(this.publisherMap);
    } catch (Exception e) {
      throw new SepException(e);
    }
  }

  @Override
  public String getReplicatorName() {
    return this.getClass().getName();
  }

  private Optional<Pair<String, Future<RecordMetadata>>> getStringFuturePair(Cell cell, String topic, String columnFamily, byte[] bytes, boolean propogateMutation, boolean localOriginFlag) {
    String kafkaProducerLabel = publisherMap.containsKey(columnFamily) ? columnFamily : DEFAULT_STR;
    logger.debug("kafka producer label {}", kafkaProducerLabel);
    if (publisherMap.containsKey(kafkaProducerLabel)) {
      byte[] groupId = new byte[cell.getRowLength()];
      System.arraycopy(cell.getRowArray(), cell.getRowOffset(), groupId, 0, groupId.length);
      logger.debug("pushing to cf {} rowKey {}", kafkaProducerLabel, groupId);
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, groupId, bytes);
      Future<RecordMetadata> response= this.publisherMap.get(kafkaProducerLabel).send(record);
      publisher.incrementMetric(propogateMutation ? (localOriginFlag ? SepMetricsPublisher.REPLICATE_MUTATION_EVENT_COMPLETE : SepMetricsPublisher.REPLICATE_ROW_MUTATION_EVENT_COMPLETE) : SepMetricsPublisher.REPLICATE_EVENT_COMPLETE);
      return Optional.of(new Pair<>(kafkaProducerLabel, response));
    }
    logger.debug("No kafka broker configured for this kafkaProducer {}", kafkaProducerLabel);
    return Optional.empty();
  }

  @Override
  public Optional<Pair<String,Future<RecordMetadata>>> send(SepMessage msg, Cell cell, String topic, String columnFamily) {
    return getStringFuturePair(cell, topic, columnFamily, msg.toByteArray(),false, false);
  }

  @Override
  public Optional<Pair<String,Future<RecordMetadata>>> send(SepMessageV2 msg, Cell cell, String topic, String columnFamily, boolean localOriginFlag) {
    return getStringFuturePair(cell, topic, columnFamily, msg.toByteArray(),true, localOriginFlag);
  }

  @Override
  public void onStart(Configuration configuration) throws SepException {
    try {
      this.hbaseConfig = configuration;
      this.reloadConfig();
    } catch (SepConfigException ex) {
      publisher.incrementErrorMetric(SepMetricsPublisher.REPLICATE_EXCEPTION, ex);
      throw new SepException(ex);
    }
  }

  @Override
  public boolean skipAndFail(ReplicateContext replicateContext) {
    if (this.publisherMap == null) {
      logger.debug("No Replication Configured, Not acking");
      return true;
    }
    return false;
  }

  @Override
  public boolean skipAndAck(ReplicateContext replicateContext) {
    if (this.publisherMap.isEmpty()) {
      logger.debug("No publisher attached for replication, blindly acking");
      publisher.incrementErrorMetric(SepMetricsPublisher.REPLICATE_EXCEPTION, EMPTY_CONFIGURATION_FOUND);
      return true;
    }
    return false;
  }
}

