package com.flipkart.yak.sep.utils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto;
import sep.shade.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SepMessageUtils {

    private SepMessageUtils() {}

    public static SepMessageProto.SepMessage buildSepMessage(SepMessageProto.SepTableName sepTable, Cell cell, WALKey key) {
        return SepMessageProto.SepMessage.newBuilder()
                .setRow(ByteString.copyFrom(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()))
                .setTable(sepTable).setColumnfamily(ByteString
                        .copyFrom(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()))
                .setQualifier(ByteString.copyFrom(cell.getQualifierArray(), cell.getQualifierOffset(),
                        cell.getQualifierLength())).setTimestamp(key.getWriteTime()).setValue(
                        ByteString.copyFrom(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()))
                .build();
    }

    public static SepMessageProto.SepMessageV2 buildSepMessageV2(SepMessageProto.SepTableName sepTable, WALKey key, List<Cell> cellList) {
        SepMessageProto.SepMessageV2.Builder message = SepMessageProto.SepMessageV2.newBuilder();
        message.setTable(sepTable).setOrigSequenceId(key.getSequenceId()).setTimestamp(key.getWriteTime()).setOrigLogSeqNum(key.getOrigLogSeqNum());
        for (Cell cell : cellList) {
            SepMessageProto.SepCell msg = SepMessageUtils.buildSepCell(cell, key);
            message.addSepCell(msg);
        }
       return message.build();
    }

    public static SepMessageProto.SepCell buildSepCell( Cell cell, WALKey key) {
        return SepMessageProto.SepCell.newBuilder()
                .setRow(ByteString.copyFrom(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()))
                .setColumnfamily(ByteString.copyFrom(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()))
                .setQualifier(ByteString.copyFrom(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()))
                .setTimestamp(key.getWriteTime()).setValue(ByteString.copyFrom(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()))
                .setType(cell.getType().toString())
                .build();
    }

    public static Set<String> getCFSet(List<Cell> cellList) {
        Set<String> columnFamilySet = new HashSet<>();
        for(Cell cell : cellList) {
            columnFamilySet.add(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
        }
        return columnFamilySet;
    }

    public static String buildErrorMessage(List<WAL.Entry> entries) {
        StringBuilder errorMessage = new StringBuilder("[");
        if (entries != null) {
            entries.forEach(entry -> {
                errorMessage.append("[ Table : ")
                        .append(entry.getKey().getTableName().getNameWithNamespaceInclAsString());
                entry.getEdit().getCells().forEach(cell -> {
                    String rowKey =
                            Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String columnFamily = Bytes
                            .toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                            cell.getQualifierLength());

                    errorMessage.append(" {rowKey:").append(rowKey).append("|columnFamily:")
                            .append(columnFamily).append("|qualifier:").append(qualifier).append("}");
                });
                errorMessage.append(" ]");
            });
        }
        return errorMessage.append("]").toString();
    }

}
