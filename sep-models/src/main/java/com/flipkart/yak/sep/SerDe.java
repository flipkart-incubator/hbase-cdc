package com.flipkart.yak.sep;

import com.flipkart.yak.sep.proto.SepMessageProto.SepMessage;
import com.flipkart.yak.sep.proto.SepMessageProto.SepCell;
import com.flipkart.yak.sep.proto.SepMessageProto.SepMessageV2;

/**
 * Abstraction for client to deserialized byte[]
 * 
 * @author gokulvanan.v
 *
 */
public enum SerDe {
    DESERIALZIER;

    public SepMessage execute(byte[] data) throws SerDeException {
        try {
            return SepMessage.parseFrom(data);
        } catch (Exception e) {
            throw new SerDeException(e);
        }
    }
    public SepCell executeSepCell(byte[] data) throws SerDeException {
        try {
            return SepCell.parseFrom(data);
        } catch (Exception e) {
            throw new SerDeException(e);
        }
    }

    public SepMessageV2 executeSepMessageV2(byte[] data) throws SerDeException {
        try {
            return SepMessageV2.parseFrom(data);
        } catch (Exception e) {
            throw new SerDeException(e);
        }
    }

}
