package com.flipkart.yak.sep;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto;

public class KafkaReplicationEndPointPropogateMutationTest extends BaseTest {

    public KafkaReplicationEndPointPropogateMutationTest() {
        conf.set("sep.kafka.config.path", "src/test/resources/sep-conf-propogate-mutation.json");
    }

    @Test public void sepMessageV2PutTest() throws Exception {

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

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
    }
    @Test public void sepMessageV2PutEmptyValuesTest() throws Exception {

        byte[] rowKey = "row1".getBytes();
        byte[] qualifier = "data".getBytes();

        doPut(cf1, rowKey, qualifier,"".getBytes());
        doPut(cf2, rowKey, qualifier,"".getBytes());

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
                    Assert.assertEquals(new String(valueBytes), "");

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
    }
    @Test public void sepMessageV2DeleteTest() throws Exception {

        byte[] rowKey = "row1".getBytes();
        byte[] qualifier = "data".getBytes();

        doDelete(cf1,rowKey,qualifier);

        try {
            List<byte[]> messages = KafkaConsumer.readMessages("yak_export", 1);

            for (byte[] message : messages) {
                SepMessageProto.SepMessageV2 sepMessageV2 = SepMessageProto.SepMessageV2.parseFrom(message);
                for(SepMessageProto.SepCell sepCell : sepMessageV2.getSepCellList()) {
                    Assert.assertTrue(sepCell.getType().equals("Delete"));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
    }
}
