package com.flipkart.yak.sep;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto;

public class KafkaReplicationEndPointTestCase extends BaseTest {
  public KafkaReplicationEndPointTestCase() {
    conf.set("sep.kafka.config.path", "src/test/resources/sep-conf.json");
  }

  @Test public void nonWhiteListedQualifierTest() throws Exception {
    byte[] column = "{Message}".getBytes();
    byte[] rowKey = "row1".getBytes();
    byte[] qualifier = "value".getBytes();

    doPut(cf1, rowKey, qualifier, column);

    try {
      KafkaConsumer.readMessages("yak_export", 1);
      Assert.assertFalse(true);

    } catch (Exception e) {
      Assert.assertTrue(true);
    }

  }

  @Test public void whiteListedQualifierTest() throws Exception {

    byte[] column = "{Message}".getBytes();
    byte[] rowKey = "row1".getBytes();
    byte[] qualifier = "data".getBytes();

    doPut(cf1, rowKey, qualifier, column);
    doPut(cf2, rowKey, qualifier, column);

    try {
      List<byte[]> messages = KafkaConsumer.readMessages("yak_export", 2);

      for (byte[] message : messages) {
        SepMessageProto.SepMessage sepMessage = SepMessageProto.SepMessage.parseFrom(message);

        byte[] cfBytes = new byte[sepMessage.getColumnfamily().size()];
        sepMessage.getColumnfamily().copyTo(cfBytes, 0);

        byte[] valueBytes = new byte[sepMessage.getValue().size()];
        sepMessage.getValue().copyTo(valueBytes, 0);

        Assert.assertTrue(new String(cfBytes).equals(new String(cf1)) || new String(cfBytes)
            .equals(new String(cf2)));
        Assert.assertEquals(new String(valueBytes), new String(column));
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertFalse(true);
    }
  }
}
