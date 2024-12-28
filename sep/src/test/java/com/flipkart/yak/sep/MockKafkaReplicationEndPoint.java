package com.flipkart.yak.sep;

import com.flipkart.yak.sep.commons.CFConfig;
import com.flipkart.yak.sep.commons.SepConfig;
import com.flipkart.yak.sep.kafka.KafkaReplicationConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MockKafkaReplicationEndPoint extends KafkaReplicationEndPoint {

    private ObjectMapper mapper = new ObjectMapper();

    private Map<String, CFConfig> getCfConfig(String confPath) throws IOException {
        SepConfig conf = mapper.readValue(new File(confPath), SepConfig.class);
        return conf.getCfConfig();
    }

    private Map<String, Object> getKafkaProps(String columFamily, String confPath) throws IOException {
        if (confPath != null) {
            Map<String, CFConfig> cfConfigMap = getCfConfig(confPath);

            for (Map.Entry<String, CFConfig> cfEntry : cfConfigMap.entrySet()) {
                String cf = cfEntry.getKey().trim();
                if (cf.equals(columFamily) || cf.equals("default")) {
                    return ((KafkaReplicationConfig)cfEntry.getValue()).getKafkaConfig();
                }
            }
        }
        return new HashMap<>();
    }

    static class MockKafkaProducer<K, V> extends KafkaProducer<K, V> {
        private int failurePercentage = 0; //
        private Random rand = new Random();

        public MockKafkaProducer(Map<String, Object> configs, int failurePercentage) {
            super(configs);
            this.failurePercentage = failurePercentage > 100 ? 100 : (failurePercentage < 0 ? 0 : failurePercentage);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
            int r = rand.nextInt(100);
            if (r > failurePercentage) {
                return super.send(record);
            } else {
                return new Future<RecordMetadata>() {
                    @Override
                    public boolean cancel(boolean b) {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return false;
                    }

                    @Override
                    public RecordMetadata get() throws InterruptedException, ExecutionException {
                        Exception ex = new Exception("Something went wrong");
                        throw new ExecutionException(ex);
                    }

                    @Override
                    public RecordMetadata get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
                        Exception ex = new Exception("Something went wrong");
                        throw new ExecutionException(ex);
                    }
                };
            }
        }
    }

    @Override
    public Map<String, Producer<byte[], byte[]>> buildKafkaPublishers(
            Map<String, KafkaReplicationConfig> cfConfigs) {

        Configuration configuration = HBaseConfiguration.create(this.ctx.getConfiguration());
        String confPath = configuration.get("sep.kafka.config.path");
        int failurePercentage = configuration.getInt("sep.kafka.failure.percent", 0);

        Map<String, Producer<byte[], byte[]>> publisherMap = new HashMap<>();
        for (Map.Entry<String, KafkaReplicationConfig> entry : cfConfigs.entrySet()) {
            publisherMap.putIfAbsent(entry.getKey(), new MockKafkaProducer<>(entry.getValue().getKafkaConfig(),failurePercentage));
        }
        return publisherMap;
    }
}
