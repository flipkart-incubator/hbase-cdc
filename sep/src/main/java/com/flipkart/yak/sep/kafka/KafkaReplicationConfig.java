package com.flipkart.yak.sep.kafka;

import com.flipkart.yak.sep.commons.CFConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.CompressionType;

import java.util.Map;

public class KafkaReplicationConfig extends CFConfig {

    private Map<String, Object> kafkaConfig;

    public Map<String, Object> getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(Map<String, Object> kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.kafkaConfig.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.LZ4.name.toLowerCase());
    }
}
