package com.flipkart.yak.sep.filters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.wal.WAL;

import java.util.UUID;

public class SepFilterModelBuilder {
    private UUID originClusterId;
    private UUID currentClusterId;
    private Cell cell;
    private WAL.Entry walEntry;
    private String qualifier;
    private String columnFamily;

    public SepFilterModelBuilder setOriginClusterId(UUID originClusterId) {
        this.originClusterId = originClusterId;
        return this;
    }

    public SepFilterModelBuilder setCurrentClusterId(UUID currentClusterId) {
        this.currentClusterId = currentClusterId;
        return this;
    }

    public SepFilterModelBuilder setCell(Cell cell) {
        this.cell = cell;
        return this;
    }

    public SepFilterModelBuilder setWalEntry(WAL.Entry walEntry) {
        this.walEntry = walEntry;
        return this;
    }

    public SepFilterModelBuilder setQualifier(String qualifier) {
        this.qualifier = qualifier;
        return this;
    }

    public SepFilterModelBuilder setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
        return this;
    }

    public SepFilterBaseModel createSepFilterBaseModel() {
        return new SepFilterBaseModel(originClusterId, currentClusterId, cell, walEntry, qualifier, columnFamily);
    }
}