package com.flipkart.yak.sep;

import org.junit.Assert;
import org.junit.Test;

public class InvalidSepConfFormatTestCase extends BaseTest {

  public InvalidSepConfFormatTestCase() {
    conf.set("sep.kafka.config.path", "src/test/resources/sep-conf-invalid-json.json");
  }

  @Test public void invalidJsonConfigTest() throws Exception {

    byte[] column = "{Message}".getBytes();
    byte[] rowKey = "row1".getBytes();
    byte[] qualifier = "data".getBytes();

    doPut(cf1, rowKey, qualifier, column);

    try {
      KafkaConsumer.readMessages("yak_export", 1);
      Assert.assertFalse(true);

    } catch (Exception e) {
      Assert.assertTrue(true);
    }

  }
}
