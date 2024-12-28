package com.flipkart.yak.sep;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.yak.sep.commons.SepConfig;
import com.flipkart.yak.sep.kafka.KafkaHandler;
import com.flipkart.yak.sep.kafka.KafkaReplicationConfig;
import com.flipkart.yak.sep.pulsar.PulsarHandler;
import com.flipkart.yak.sep.pulsar.PulsarReplicationConfig;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

public class SepConfigDeserializeTestCase {

  @Test public void testReadingKafkaConf() throws IOException {

    ObjectMapper mapper = new ObjectMapper();
    SepConfig<KafkaReplicationConfig> conf=mapper.readValue(new File("src/test/resources/sep-conf.json"), new TypeReference<SepConfig< KafkaReplicationConfig >>(){});
    HashSet<String> set = new HashSet();
    set.add("data");
    set.add("events");
    Assert.assertArrayEquals(conf.getCfConfig().get("default").getWhiteListedQualifier().toArray(), set.toArray());
  }

  @Test public void testReadingEmptyKafkaConf() throws IOException {

    ObjectMapper mapper = new ObjectMapper();
    SepConfig<KafkaReplicationConfig> conf=mapper.readValue(new File("src/test/resources/sep-conf-empty.json"), new TypeReference<SepConfig< KafkaReplicationConfig >>(){});
    Map<String, KafkaReplicationConfig> mapConfig = conf.getCfConfig();
    Map<String, Producer<byte[], byte[]>> producerMap = KafkaHandler.buildKafkaPublishers(mapConfig);
    Assert.assertTrue(producerMap.isEmpty());
  }

  @Test public void testReadingEmptyPulsarConf() throws IOException {

    ObjectMapper mapper = new ObjectMapper();
    SepConfig<PulsarReplicationConfig> conf= mapper.readValue(new File("src/test/resources/sep-pulsar-conf-empty.json"), new TypeReference<SepConfig<PulsarReplicationConfig>>(){});
    Map<String, PulsarReplicationConfig> mapConfig = conf.getCfConfig();
    assert mapConfig != null;
    PulsarHandler.buildPublisherMap(conf);
  }

  @Test public void testReadingPulsarConf() throws IOException {

    ObjectMapper mapper = new ObjectMapper();
    SepConfig<PulsarReplicationConfig> conf= mapper.readValue(new File("src/test/resources/sep-conf-pulsar.json"), new TypeReference<SepConfig<PulsarReplicationConfig>>(){});
    Assert.assertEquals(conf.getCfConfig().get("default").getClientId(), "clientid");
    HashSet<String> set = new HashSet();
    set.add("data");
    set.add("events");
    Assert.assertArrayEquals(conf.getCfConfig().get("default").getWhiteListedQualifier().toArray(), set.toArray());
  }

}
