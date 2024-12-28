package com.flipkart.yak.sep;

import org.junit.Assert;
import org.junit.Test;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto;

import java.util.List;

public class KafkaFailuresPropogateMutationTestCase extends BaseTest {

    private static final String confPath = "src/test/resources/sep-conf-propogate-mutation.json";

    public KafkaFailuresPropogateMutationTestCase() {
        conf.set("sep.kafka.config.path", confPath);
        conf.set("sep.kafka.failure.percent", "100");
    }

    @Override
    String getReplicationEndpoint() {
        return MockKafkaReplicationEndPoint.class.getName();
    }

    @Test
    public void kafkaPutExceptionTest() throws Exception {
        byte[] column = "{Message}".getBytes();
        byte[] rowKey = "row1".getBytes();
        byte[] qualifier = "data".getBytes();

        doPut(cf1, rowKey, qualifier, column);
        doPut(cf2, rowKey, qualifier, column);

        try {
            List<byte[]> messages = KafkaConsumer.readMessages("yak_export", 2);

            for (byte[] message : messages) {
                SepMessageProto.SepMessageV2 sepMessageV2 = SepMessageProto.SepMessageV2.parseFrom(message);
                for(SepMessageProto.SepCell sepCell : sepMessageV2.getSepCellList())
                {
                    byte[] cfBytes = new byte[sepCell.getColumnfamily().size()];
                    sepCell.getColumnfamily().copyTo(cfBytes, 0);
                    byte[] valueBytes = new byte[sepCell.getValue().size()];
                    sepCell.getValue().copyTo(valueBytes, 0);
                    Assert.assertTrue(sepCell.getType().equals("Put"));
                    Assert.assertTrue(new String(cfBytes).equals(new String(cf1)) || new String(cfBytes)
                            .equals(new String(cf2)));
                    Assert.assertEquals(new String(valueBytes), new String(column));

                }
            }
            Assert.assertFalse(true); // Failure
        } catch (Exception e) {
            Assert.assertTrue(true); // Success
        }
    }

    @Test
    public void kafkaDeleteExceptionTest() throws Exception {
        byte[] rowKey = "row1".getBytes();
        byte[] qualifier = "data".getBytes();

        doDelete(cf1,rowKey,qualifier);

        try {
            List<byte[]> messages = KafkaConsumer.readMessages("yak_export", 2);

            for (byte[] message : messages) {
                SepMessageProto.SepMessageV2 sepMessageV2 = SepMessageProto.SepMessageV2.parseFrom(message);
                for(SepMessageProto.SepCell sepCell : sepMessageV2.getSepCellList()) {
                    Assert.assertTrue(sepCell.getType().equals("Delete"));
                }
            }
            Assert.assertFalse(true); // Failure
        } catch (Exception e) {
            Assert.assertTrue(true); // Success
        }
    }


}
