package com.flipkart.yak.sep.pulsar;

import com.flipkart.yak.sep.commons.SepConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.url.DataURLStreamHandler;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.shade.com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PulsarHandler {

    private PulsarHandler() {}

    private static final int STAT_INTERVAL = 5;
    private static final String CLIENT_AUDIENCE_IDENTIFIER = "yak";
    private static final String TYPE_KEY = "type";
    private static final String CLIENT_ID_KEY = "client_id";
    private static final String CLIENT_SECRET_EKY = "client_secret";
    private static final String ISSUER_URL_KEY = "issuer_url";
    private static final String PRODUCER_NAME_PREFIX = "-PULSAR-";
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarHandler.class);
    private static final Map<String, PulsarClient> endpointToClientMap = new ConcurrentHashMap<>();
    private static final Map<String, Producer<byte[]>> pulsarPublisherMap = new ConcurrentHashMap<>();

    private static PulsarClient createClient(PulsarReplicationConfig config) throws PulsarClientException {
        try {
            String clientId = config.getClientId();
            String clientSecret = config.getClientSecret();
            String issuerUrl = config.getAuthNEndPoint();
            int connectionsPerBroker = config.getConnectionsPerBroker();
            URL credentialsUrl = buildClientCredentials(clientId, clientSecret, issuerUrl);
            Authentication authn = AuthenticationFactoryOAuth2.clientCredentials(new URL(issuerUrl), credentialsUrl, CLIENT_AUDIENCE_IDENTIFIER);
            return PulsarClient.builder()
                    .serviceUrl(config.getEndPoint())
                    .authentication(authn)
                    .statsInterval(STAT_INTERVAL, TimeUnit.SECONDS)
                    .connectionsPerBroker(connectionsPerBroker)
                    .build();
        } catch (MalformedURLException ex) {
            throw new PulsarClientException(ex);
        }
    }

    private static URL buildClientCredentials(String clientId, String clientSecret, String issuerUrl)
            throws MalformedURLException {
        Map<String, String> props = new HashMap<>();
        props.put(TYPE_KEY, "client_credentials");
        props.put(CLIENT_ID_KEY, clientId);
        props.put(CLIENT_SECRET_EKY, clientSecret);
        props.put(ISSUER_URL_KEY, issuerUrl);
        Gson gson = new Gson();
        String json = gson.toJson(props);
        String encoded = Base64.getEncoder().encodeToString(json.getBytes());
        String data = "data:application/json;base64," + encoded;
        return new URL(null,  data, new DataURLStreamHandler());
    }

    public static Producer<byte[]> createProducer(PulsarClient client, String topicName,
                                                  PulsarReplicationConfig pulsarReplicationConfigSepConfig) throws
            PulsarClientException, UnknownHostException {
        String hostname = InetAddress.getLocalHost().getHostName();
        return client.newProducer().topic(topicName)
                .producerName(hostname+PRODUCER_NAME_PREFIX+topicName)
                .enableBatching(pulsarReplicationConfigSepConfig.isBatchMessage())
                .batchingMaxMessages(pulsarReplicationConfigSepConfig.getMaxMessagesInABatch())
                .maxPendingMessages(pulsarReplicationConfigSepConfig.getMaxPendingMessage())
                .blockIfQueueFull(pulsarReplicationConfigSepConfig.isBlockIfQueueIsFull())
                .batchingMaxPublishDelay(pulsarReplicationConfigSepConfig.getMaxBatchPublishDelayInMillis(), TimeUnit.MILLISECONDS)
                .sendTimeout(pulsarReplicationConfigSepConfig.getTimeOut(), TimeUnit.SECONDS)
                .compressionType(pulsarReplicationConfigSepConfig.getCompressionType())
                .create();
    }

    public static void addIfAbsent(String topic, String columnFamily, SepConfig<PulsarReplicationConfig> pulsarReplicationConfigSepConfig) throws UnknownHostException, PulsarClientException {
        if(!pulsarReplicationConfigSepConfig.getCfConfig().containsKey(columnFamily)) {
            LOGGER.error("no columnFamily config specified for this column Key {}, messages will not be published for this topic", columnFamily);
            return;
        }
        if(!endpointToClientMap.containsKey(columnFamily)) {
            LOGGER.error("no pulsar endpoint configured for this cf {}, messages will not be published for this topic", columnFamily);
            return;
        }
        if( !pulsarPublisherMap.containsKey(topic)) {
            LOGGER.info("Adding Producer for topic {}", topic);
            PulsarReplicationConfig replicationConfig = pulsarReplicationConfigSepConfig.getCfConfig().get(columnFamily);
            Producer<byte[]> producer = createProducer(endpointToClientMap.get(columnFamily),
                    topic, replicationConfig);
            LOGGER.info("creating pulsar producer for topic {} with name {}", topic,
                    producer.getProducerName());
            pulsarPublisherMap.putIfAbsent(topic, producer);
        }

    }


    public static Map<String, Producer<byte[]>> getPulsarPublisherMap() {
        return pulsarPublisherMap;
    }

    public static void buildPublisherMap(SepConfig<PulsarReplicationConfig> repl) {
        try {
            if (!repl.getCfConfig().isEmpty()) {
            for (Map.Entry<String, PulsarReplicationConfig> cfEntry : repl.getCfConfig().entrySet()) {
                String cf = cfEntry.getKey().trim();
                PulsarReplicationConfig replicationConfig = cfEntry.getValue();
                if (!cf.isEmpty()) {
                    LOGGER.info("creating pulsar producer for column family {}", cf);
                    endpointToClientMap.putIfAbsent(cf, createClient(replicationConfig));
                    for (String topicName : replicationConfig.getQualifierToTopicNameMap().values()) {
                        if (!pulsarPublisherMap.containsKey(topicName)) {
                            Producer<byte[]> producer = createProducer(endpointToClientMap.get(cf),
                                    topicName, replicationConfig);
                            LOGGER.info("creating pulsar producer for topic {} with name {}", topicName,
                                    producer.getProducerName());
                            pulsarPublisherMap.put(topicName, producer);
                        } else {
                            LOGGER.info("Producer for topic:{} already present, ignoring!", topicName);
                        }

                    }
                    if (StringUtils.isNotBlank(replicationConfig.getDefaultTopicName())) {
                        if (!pulsarPublisherMap.containsKey(replicationConfig.getDefaultTopicName())) {
                            Producer<byte[]> producer = createProducer(endpointToClientMap.get(cf),
                                    replicationConfig.getDefaultTopicName(), replicationConfig);
                            LOGGER.info("creating default pulsar producer for topic {} with name {}", replicationConfig.getDefaultTopicName(),
                                    producer.getProducerName());
                            pulsarPublisherMap.put(replicationConfig.getDefaultTopicName(), producer);
                        }
                        else {
                            LOGGER.info("Producer for default topic:{} already present, ignoring!",
                                    replicationConfig.getDefaultTopicName());
                        }
                    }
                }
            }
            }
        } catch (PulsarClientException e) {
            LOGGER.error("Could not create producer map, Received {}", e.getMessage(), e);
        } catch (UnknownHostException e) {
            LOGGER.error("Could Not resolve localhost name {}", e.getMessage(), e);
        }
        LOGGER.info("pulsar producer map contains producers for {}", pulsarPublisherMap.keySet());
    }

    public static void closeProducers() throws PulsarClientException {
        for(Map.Entry<String, Producer<byte[]>> producer:  pulsarPublisherMap.entrySet()) {
            Producer<byte[]> thisProducer = producer.getValue();
            try {
                thisProducer.close();
                LOGGER.info("Producer state for {}: {}", thisProducer.getTopic(), thisProducer.isConnected());
            } catch (PulsarClientException e) {
                LOGGER.error("could not close producer for {}", producer.getKey(), e);
            }
        }
        for(Map.Entry<String, PulsarClient> client:  endpointToClientMap.entrySet()) {
            PulsarClient pulsarClient = client.getValue();
            try {
                pulsarClient.close();
            } catch (PulsarClientException e) {
                LOGGER.error("could not close client for {}", client.getKey(), e);
            }
        }
        LOGGER.info("closed all producers!");
        endpointToClientMap.clear();
        pulsarPublisherMap.clear();
    }

}
