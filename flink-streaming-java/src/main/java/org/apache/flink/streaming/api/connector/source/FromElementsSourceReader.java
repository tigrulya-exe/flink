package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class FromElementsSourceReader<T> implements SourceReader<T, FromElementsSplit> {
    /** The context for this reader, to communicate with the enumerator. */
    private final SourceReaderContext context;

    /** The availability future. This reader is available as soon as a split is assigned. */
    private CompletableFuture<Void> availability;

    private final Iterable<T> elements;
    private final Iterator<T> elementsIter;

    private final int elementsCount;

    private int currentOffset;

    private FromElementsSplit currentSplit;

    private boolean noMoreSplits;

    /** The remaining splits that were assigned but not yet processed. */
    private final Queue<FromElementsSplit> remainingSplits;

    public FromElementsSourceReader(
            SourceReaderContext context,
            Iterable<T> elements) {
        this.context = context;
        this.elements = elements;
        this.elementsIter = elements.iterator();
        this.elementsCount = Iterables.size(elements);
        this.availability = new CompletableFuture<>();
        this.remainingSplits = new ArrayDeque<>();
    }

    @Override
    public void start() {
        if (remainingSplits.isEmpty()) {
            context.sendSplitRequest();
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (currentSplit != null) {
            if (elementsIter.hasNext()) {
                output.collect(elementsIter.next());
            }
        }

        return null;
    }

    @Override
    public List<FromElementsSplit> snapshotState(long checkpointId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    @Override
    public void addSplits(List<FromElementsSplit> splits) {
        remainingSplits.addAll(splits);
        // set availability so that pollNext is actually called
        availability.complete(null);
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
        // set availability so that pollNext is actually called
        availability.complete(null);
    }

    @Override
    public void close() throws Exception {}
}
