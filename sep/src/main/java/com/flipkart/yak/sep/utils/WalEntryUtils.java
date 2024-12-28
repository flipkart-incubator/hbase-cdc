package com.flipkart.yak.sep.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto;
import sep.shade.com.google.protobuf.ByteString;

import java.util.List;
import java.util.UUID;

public class WalEntryUtils {

    private WalEntryUtils() {}

    public static UUID getOriginClusterIdFromWAL(WAL.Entry walEntry) {
        WALKeyImpl key = (WALKeyImpl) WalEntryUtils.getWALKeyFromEntry(walEntry);
        List<UUID> originClusterIds = key.getClusterIds();
        UUID originClusterId = null;
        if (originClusterIds != null && !originClusterIds.isEmpty()) {
            originClusterId = originClusterIds.get(0);
        }
        return originClusterId;
    }

    public static SepMessageProto.SepTableName getSepTableName(WAL.Entry entry) {
        WALKey key = WalEntryUtils.getWALKeyFromEntry(entry);
        TableName tableName = key.getTableName();
        SepMessageProto.SepTableName sepTable =
                SepMessageProto.SepTableName.newBuilder().setNamespace(ByteString.copyFrom(tableName.getNamespace()))
                        .setQualifier(ByteString.copyFrom(tableName.getName())).build();
        return sepTable;

    }

    public static WALKey getWALKeyFromEntry(WAL.Entry entry) {
        return entry.getKey();
    }

    public  static WALEdit getWALEditFromEntry(WAL.Entry entry) {
        WALEdit edit = entry.getEdit();
        return edit;
    }

}
