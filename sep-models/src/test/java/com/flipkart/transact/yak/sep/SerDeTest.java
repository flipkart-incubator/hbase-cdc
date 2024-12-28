package com.flipkart.transact.yak.sep;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.flipkart.yak.sep.SerDe;
import com.flipkart.yak.sep.SerDeException;
import com.flipkart.yak.sep.proto.SepMessageProto.SepMessage;
import com.flipkart.yak.sep.proto.SepMessageProto.SepTableName;
import com.flipkart.yak.sep.proto.SepMessageProto.SepCell;
import com.flipkart.yak.sep.proto.SepMessageProto.SepMessageV2;
import com.google.protobuf.ByteString;

public class SerDeTest {

    @Test
    public void test() {
        SepTableName sepTable = SepTableName.newBuilder()
                .setNamespace(ByteString.copyFrom("test".getBytes()))
                .setQualifier(ByteString.copyFrom("new_table".getBytes()))
                .build();
        SepMessage msg = SepMessage
                .newBuilder()
                .setTable(sepTable)
                .setTimestamp(System.currentTimeMillis())
                .setRow(ByteString.copyFrom(UUID.randomUUID().toString()
                        .getBytes()))
                .setValue(ByteString.copyFrom("Sample value".getBytes()))
                .setColumnfamily(ByteString.copyFrom("cf".getBytes()))
                .setQualifier(ByteString.copyFrom("data".getBytes()))
                .build();
        byte[] data = msg.toByteArray();

        try {
            SepMessage newMsg = SerDe.DESERIALZIER.execute(data);
            Assert.assertEquals(msg, newMsg);
        } catch (SerDeException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testSepCellDeserializer() {
        SepCell msg = SepCell.newBuilder()
                .setRow(ByteString.copyFrom(UUID.randomUUID().toString()
                        .getBytes()))
                .setTimestamp(System.currentTimeMillis())
                .setValue(ByteString.copyFrom("Sample value".getBytes()))
                .setColumnfamily(ByteString.copyFrom("cf".getBytes()))
                .setQualifier(ByteString.copyFrom("data".getBytes()))
                .setType("Put")
                .build();
        byte[] data = msg.toByteArray();

        try {
            SepCell newMsg = SerDe.DESERIALZIER.executeSepCell(data);
            Assert.assertEquals(msg, newMsg);
        } catch (SerDeException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testSepMessageV2Deserializer() {
        SepTableName sepTable = SepTableName.newBuilder()
                .setNamespace(ByteString.copyFrom("test".getBytes()))
                .setQualifier(ByteString.copyFrom("new_table".getBytes()))
                .build();
        SepCell sepCell = SepCell.newBuilder()
                .setRow(ByteString.copyFrom(UUID.randomUUID().toString()
                        .getBytes()))
                .setTimestamp(System.currentTimeMillis())
                .setValue(ByteString.copyFrom("Sample value".getBytes()))
                .setColumnfamily(ByteString.copyFrom("cf".getBytes()))
                .setQualifier(ByteString.copyFrom("data".getBytes()))
                .setType("Put")
                .build();
        SepMessageV2 msg = SepMessageV2
                .newBuilder()
                .setTable(sepTable)
                .addSepCell(sepCell)
                .setOrigSequenceId(67338888l)
                .setOrigLogSeqNum(9783498l)
                .setTimestamp(System.currentTimeMillis())
                .build();
        byte[] data = msg.toByteArray();

        try {
            SepMessageV2 newMsg = SerDe.DESERIALZIER.executeSepMessageV2(data);
            Assert.assertEquals(msg, newMsg);
        } catch (SerDeException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
