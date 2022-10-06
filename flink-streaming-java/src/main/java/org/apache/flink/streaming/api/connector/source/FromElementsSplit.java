package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.UUID;

public class FromElementsSplit implements SourceSplit, Serializable {
    private final String splitId;
    private int offset;

    public FromElementsSplit(int offset) {
        this.splitId = UUID.randomUUID().toString();
        this.offset = offset;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public int getOffset() {
        return offset;
    }
}
