package com.flipkart.yak.sep;

import org.junit.Assert;
import org.junit.Test;

public class KafkaReplicationEndPointSourceFilteringTest extends BaseTest {
  public KafkaReplicationEndPointSourceFilteringTest() {
    conf.set("sep.kafka.config.path", "src/test/resources/sep-conf-inter-cluster.json");
    shouldSetupSecondCluster = true;
  }

  @Test public void onlyWhitelistedOriginsReplicateWithoutWhitelistedClusters() throws Exception {
    byte[] column = "{Message}".getBytes();
    byte[] rowKey = "row1".getBytes();
    byte[] qualifier = "data".getBytes();

    doPut(cf3, rowKey, qualifier, column);

    try {
      KafkaConsumer.readMessages("yak_export", 1);
      Assert.assertFalse("This message is suppose to be filtered out", true);
    } catch (Exception e) {
      Assert.assertTrue(true);
    }
  }

  @Test public void onlyWhitelistedOriginsReplicateWithWhitelistedCluster() throws Exception {
    byte[] column = "{Message}".getBytes();
    byte[] rowKey = "row1".getBytes();
    byte[] qualifier = "data".getBytes();

    doPut2(cf4, rowKey, qualifier, column);

    try {
      KafkaConsumer.readMessages("yak_export", 1);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse("This message is not suppose to be filtered out", true);
    }

    doPut(cf4, rowKey, qualifier, column);

    try {
      KafkaConsumer.readMessages("yak_export", 1);
      Assert.assertFalse("This message is suppose to be filtered out", true);
    } catch (Exception e) {
      Assert.assertTrue(true);
    }
  }

  @Test public void anyOriginsToReplicate() throws Exception {
    byte[] column = "{Message}".getBytes();
    byte[] rowKey = "row1".getBytes();
    byte[] qualifier = "data".getBytes();

    doPut(cf2, rowKey, qualifier, column);
    doPut2(cf2, rowKey, qualifier, column);

    try {
      KafkaConsumer.readMessages("yak_export", 2);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse("This message is suppose to be not filtered out", true);
    }
  }

  @Test public void sourceOriginToNotReplicate() throws Exception {
    byte[] column = "{Message}".getBytes();
    byte[] rowKey = "row1".getBytes();
    byte[] qualifier = "data".getBytes();

    doPut(cf1, rowKey, qualifier, column);

    try {
      KafkaConsumer.readMessages("yak_export", 1);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse("This message is suppose to not filter out", true);
    }

    doPut2(cf1, rowKey, qualifier, column);

    try {
      KafkaConsumer.readMessages("yak_export", 1);
      Assert.assertFalse("This message is suppose to be filtered out", true);
    } catch (Exception e) {
      Assert.assertTrue(true);
    }
  }

  String getReplicationEndpoint() {
    return MockKafkaReplicationEndPoint.class.getName();
  }
}
