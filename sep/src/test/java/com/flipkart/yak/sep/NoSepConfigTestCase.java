package com.flipkart.yak.sep;

import org.junit.Assert;
import org.junit.Test;

public class NoSepConfigTestCase extends BaseTest {

  @Test public void noConfigTest() throws Exception {

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
