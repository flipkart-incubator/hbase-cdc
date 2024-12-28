package com.flipkart.yak.sep;

import org.junit.Assert;
import org.junit.Test;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto;

import java.util.List;

public class KafkaFailuresTestCase extends BaseTest {

    private static final String confPath = "src/test/resources/sep-conf.json";

    public KafkaFailuresTestCase() {
        conf.set("sep.kafka.config.path", confPath);
        conf.set("sep.kafka.failure.percent", "100");
    }

    @Override
    String getReplicationEndpoint() {
        return MockKafkaReplicationEndPoint.class.getName();
    }

    @Test
    public void kafkaAnyExceptionTest() throws Exception {
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
            Assert.assertFalse(true); // Failure
        } catch (Exception e) {
            Assert.assertTrue(true); // Success
        }
    }
}
