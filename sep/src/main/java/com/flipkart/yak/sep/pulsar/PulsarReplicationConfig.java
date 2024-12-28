package com.flipkart.yak.sep.pulsar;

import com.flipkart.yak.sep.commons.CFConfig;
import org.apache.pulsar.client.api.CompressionType;

import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_BATCHING_MAX_MESSAGES;
import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_MAX_PENDING_MESSAGES;
import static org.apache.pulsar.client.api.CompressionType.*;

public class PulsarReplicationConfig extends CFConfig {
    private String endPoint;
    private String authNEndPoint;
    private String clientId;
    private String clientSecret;
    private int timeOut = 5;
    private int connectionsPerBroker = 3;
    private CompressionType compressionType = LZ4;

    private long maxBatchPublishDelayInMillis = 500;

    private int maxPendingMessage = DEFAULT_MAX_PENDING_MESSAGES;

    private int maxMessagesInABatch = DEFAULT_BATCHING_MAX_MESSAGES;
    private boolean blockIfQueueIsFull=true;

    public int getMaxMessagesInABatch() {
        return maxMessagesInABatch;
    }

    public void setMaxMessagesInABatch(int maxMessagesInABatch) {
        this.maxMessagesInABatch = maxMessagesInABatch;
    }

    private boolean batchMessage=true;

    public boolean isBatchMessage() {
        return batchMessage;
    }

    public void setBatchMessage(boolean batchMessage) {
        this.batchMessage = batchMessage;
    }

    public int getMaxPendingMessage() {
        return maxPendingMessage;
    }

    public void setMaxPendingMessage(int maxPendingMessage) {
        this.maxPendingMessage = maxPendingMessage;
    }

    public boolean isBlockIfQueueIsFull() {
        return blockIfQueueIsFull;
    }

    public void setBlockIfQueueIsFull(boolean blockIfQueueIsFull) {
        this.blockIfQueueIsFull = blockIfQueueIsFull;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public String getAuthNEndPoint() {
        return authNEndPoint;
    }

    public void setAuthNEndPoint(String authNEndPoint) {
        this.authNEndPoint = authNEndPoint;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    public long getMaxBatchPublishDelayInMillis() {
        return maxBatchPublishDelayInMillis;
    }

    public void setMaxBatchPublishDelayInMillis(long maxBatchPublishDelayInMillis) {
        this.maxBatchPublishDelayInMillis = maxBatchPublishDelayInMillis;
    }

    public int getConnectionsPerBroker() {
        return connectionsPerBroker;
    }

    public void setConnectionsPerBroker(int connectionsPerBroker) {
        this.connectionsPerBroker = connectionsPerBroker;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }
}
