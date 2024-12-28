package com.flipkart.yak.sep;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.yak.sep.commons.MessageQueueReplicationEndpoint;
import com.flipkart.yak.sep.commons.SepConfig;
import com.flipkart.yak.sep.commons.exceptions.SepConfigException;
import com.flipkart.yak.sep.commons.exceptions.SepException;
import com.flipkart.yak.sep.metrics.SepMetricsPublisher;
import com.flipkart.yak.sep.pulsar.PulsarHandler;
import com.flipkart.yak.sep.pulsar.PulsarReplicationConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Implementation to replicate edits to Apache Pulsar queues. Requires mandatory AuthN authorization to produce to topic.
 */
public class PulsarReplicationEndpoint extends MessageQueueReplicationEndpoint<PulsarReplicationConfig, MessageId> {


    protected SepConfig<PulsarReplicationConfig> pulsarReplicationConfigSepConfig;
    private static final Logger logger = LoggerFactory.getLogger(PulsarReplicationEndpoint.class);
    protected Configuration hbaseConfig;
    protected final ObjectMapper mapper = new ObjectMapper();

    private Optional<Pair<String, Future<MessageId>>> getStringFuturePair(Cell cell, String topic, String columnFamily, byte[] bytes, boolean propogateMutation, boolean localOriginFlag) {
        Map<String, Producer<byte[]>> producerMap = PulsarHandler.getPulsarPublisherMap();
        String producerLabel = topic;
        if(!producerMap.containsKey(producerLabel)) {
            logger.debug("Publisher for topic {} is not present, trying to create one!", producerLabel);
            try {
                PulsarHandler.addIfAbsent(producerLabel, columnFamily, this.pulsarReplicationConfigSepConfig);
            } catch (UnknownHostException | PulsarClientException e) {
               logger.error("Could not create producer for {}, error: {}", columnFamily, e.getMessage());
               publisher.incrementErrorMetric(SepMetricsPublisher.REPLICATE_EXCEPTION,e);
            }
        }
        logger.debug("Pulsar producer label {}", producerLabel);
        if (producerMap.containsKey(producerLabel)) {
            byte[] groupId = new byte[cell.getRowLength()];
            System.arraycopy(cell.getRowArray(), cell.getRowOffset(), groupId, 0, groupId.length);
            logger.debug("pushing to cf {} rowKey {}", producerLabel, groupId);
            CompletableFuture<MessageId> response = producerMap.get(producerLabel).newMessage().keyBytes(groupId)
                    .value(bytes).sendAsync();
            publisher.incrementMetric(propogateMutation ? (localOriginFlag ? SepMetricsPublisher.REPLICATE_MUTATION_EVENT_COMPLETE : SepMetricsPublisher.REPLICATE_ROW_MUTATION_EVENT_COMPLETE) : SepMetricsPublisher.REPLICATE_EVENT_COMPLETE);
            return Optional.of(new Pair<>(producerLabel, response));
        }
        logger.debug("No Pulsar broker configured for this Producer {}", producerLabel);
        return Optional.empty();
    }

    @Override
    public Optional<Pair<String, Future<MessageId>>> send(SepMessageProto.SepMessage sepMessage, Cell cell, String topic, String columnFamily) {
        return getStringFuturePair(cell, topic, columnFamily, sepMessage.toByteArray(), false, false);
    }

    @Override
    public Optional<Pair<String, Future<MessageId>>> send(SepMessageProto.SepMessageV2 sepMessage, Cell cell, String topic, String columnFamily, boolean localOriginFlag) {
        return getStringFuturePair(cell, topic, columnFamily, sepMessage.toByteArray(), true, localOriginFlag);
    }

    @Override
    public void onStart(Configuration hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
        try {
            this.stopReplication();
            this.reloadConfig();
        } catch (Exception e) {
            logger.error("could not load config {}", e.getMessage(), e);
        }
    }

    private void loadProducerFromConfig() {
        if (this.pulsarReplicationConfigSepConfig != null) {
            PulsarHandler.buildPublisherMap(this.pulsarReplicationConfigSepConfig);
            if (PulsarHandler.getPulsarPublisherMap().isEmpty()) {
                logger.warn("No producer configured ");
            }
        }
    }

    @Override
    public boolean skipAndFail(ReplicateContext replicateContext) {
        if(this.pulsarReplicationConfigSepConfig == null) {
            logger.debug("No Replication Config Provided or loaded, Not acking");
            publisher.incrementErrorMetric(SepMetricsPublisher.REPLICATE_EXCEPTION, EMPTY_CONFIGURATION_FOUND);
            return true;
        }
        return false;
    }

    @Override
    public boolean skipAndAck(ReplicateContext replicateContext) {
        if (this.pulsarReplicationConfigSepConfig.getCfConfig().isEmpty()) {
            logger.debug("No publisher configuration given for replication, assuming replication not required, blindly acking");
            publisher.incrementErrorMetric(SepMetricsPublisher.REPLICATE_EXCEPTION, EMPTY_CONFIGURATION_FOUND);
            return true;
        }
        return false;
    }

    @Override
    public String getReplicatorName() {
        return "PULSAR";
    }

    @Override
    public void reloadConfig() throws SepConfigException {
        String confPath = this.hbaseConfig.get("sep.pulsar.config.path");
        if (confPath != null) {
            try {
                this.pulsarReplicationConfigSepConfig = mapper.readValue(new File(confPath),  new TypeReference<SepConfig<PulsarReplicationConfig>>() {});
            } catch (IOException e) {
                throw new SepConfigException(e);
            }
            loadProducerFromConfig();
            logger.info("Configuration read: {}" , this.pulsarReplicationConfigSepConfig.getCfConfig());
            this.cfConfigMap = this.pulsarReplicationConfigSepConfig.getCfConfig();
            this.propogateMutationFlag = this.pulsarReplicationConfigSepConfig.getPropogateMutation();
            this.cbConfig = this.pulsarReplicationConfigSepConfig.getCbConfig();
            logger.info("Producers created successfully..");
        }
    }

    @Override
    public void stopReplication() throws SepException {
        try {
            PulsarHandler.closeProducers();
        } catch (PulsarClientException e) {
            throw new SepException(e);
        }
    }

}
