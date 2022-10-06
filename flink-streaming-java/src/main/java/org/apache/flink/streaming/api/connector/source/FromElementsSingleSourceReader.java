package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FromElementsSingleSourceReader <T> implements SourceReader<T, FromElementsSplit> {

    private final SourceReaderContext context;

    /** The availability future. This reader is available as soon as a split is assigned. */
    private CompletableFuture<Void> availability;

    private final byte[] serializedElements;
    private final int elementsCount;
    private int currentOffset;

    public FromElementsSingleSourceReader(
            SourceReaderContext context,
            byte[] serializedElements,
            int elementsCount) {
        this.context = context;
        this.serializedElements = serializedElements;
        this.elementsCount = elementsCount;
        this.availability = new CompletableFuture<>();
    }

    @Override
    public void start() {

    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        return null;
    }

    @Override
    public List<FromElementsSplit> snapshotState(long checkpointId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return null;
    }

    @Override
    public void addSplits(List<FromElementsSplit> splits) {

    }

    @Override
    public void notifyNoMoreSplits() {

    }

    @Override
    public void close() throws Exception {

    }
}
