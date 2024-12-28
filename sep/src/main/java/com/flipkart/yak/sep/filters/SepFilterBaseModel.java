package com.flipkart.yak.sep.filters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.wal.WAL;

import java.util.UUID;

public class SepFilterBaseModel {
    private UUID originClusterId;
    private UUID currentClusterId;
    private Cell cell;
    private WAL.Entry walEntry;
    private String qualifier;
    private String columnFamily;

    public UUID getOriginClusterId() {
        return originClusterId;
    }

    public void setOriginClusterId(UUID originClusterId) {
        this.originClusterId = originClusterId;
    }

    public UUID getCurrentClusterId() {
        return currentClusterId;
    }

    public void setCurrentClusterId(UUID currentClusterId) {
        this.currentClusterId = currentClusterId;
    }

    public Cell getCell() {
        return cell;
    }

    public void setCell(Cell cell) {
        this.cell = cell;
    }

    public WAL.Entry getWalEntry() {
        return walEntry;
    }

    public void setWalEntry(WAL.Entry walEntry) {
        this.walEntry = walEntry;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    SepFilterBaseModel(UUID originClusterId, UUID currentClusterId, Cell cell, WAL.Entry walEntry, String qualifier, String columnFamily) {
        this.originClusterId = originClusterId;
        this.currentClusterId = currentClusterId;
        this.cell = cell;
        this.walEntry = walEntry;
        this.qualifier = qualifier;
        this.columnFamily = columnFamily;
    }
}
