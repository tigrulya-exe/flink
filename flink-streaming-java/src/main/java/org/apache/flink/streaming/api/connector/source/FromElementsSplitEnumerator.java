package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class FromElementsSplitEnumerator<T>
        implements SplitEnumerator<CollectionSplit<T>, Collection<CollectionSplit<T>>> {

    private final SplitEnumeratorContext<CollectionSplit<T>> context;
    private final Queue<CollectionSplit<T>> remainingSplits;

    public FromElementsSplitEnumerator(
            SplitEnumeratorContext<CollectionSplit<T>> context,
            Collection<CollectionSplit<T>> splits) {
        this.context = checkNotNull(context);
        this.remainingSplits = new ArrayDeque<>(splits);
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final CollectionSplit<T> nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<CollectionSplit<T>> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public Collection<CollectionSplit<T>> snapshotState(long checkpointId) throws Exception {
        return remainingSplits;
    }

    @Override
    public void close() throws IOException {}
}
